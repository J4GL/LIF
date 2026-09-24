"""A loopback uTP responder in a thread; the BitTorrent side reuses FakeMetadataPeer.handle_connection."""
import os
import socket
import threading
import time

from dht_scraper.utp_transport import ST_DATA, ST_FIN, ST_RESET, ST_STATE, ST_SYN, decode_utp_packet, encode_utp_packet
from tests.fetch.fake_metadata_peer import FakeMetadataPeer

RESEND_SECONDS = 0.3
MODES = ("ok", "reorder", "lossy")


class FakeUtpConnection:
    """Responder side of one uTP connection, with blocking recv/sendall for the protocol thread."""

    def __init__(self, peer, address, syn):
        self.peer = peer
        self.address = address
        self.send_id = syn.connection_id
        self.recv_id = (syn.connection_id + 1) & 0xFFFF
        self.seq_nr = int.from_bytes(os.urandom(2), "big")
        self.ack_nr = syn.seq_nr
        self.buffer = bytearray()
        self.condition = threading.Condition()
        self.unacked = {}
        self.pending_pair = []
        self.sent_count = 0
        self.closed = False

    def packet(self, packet_type, seq_nr, payload=b""):
        return encode_utp_packet(packet_type, self.send_id, 0, 0, 1 << 20, seq_nr, self.ack_nr, payload)

    def on_packet(self, packet):
        if packet.packet_type in (ST_RESET, ST_FIN):
            with self.condition:
                self.closed = True
                self.condition.notify_all()
            return
        for seq in [seq for seq in list(self.unacked) if ((packet.ack_nr - seq) & 0xFFFF) < 0x8000]:
            del self.unacked[seq]
        if packet.packet_type != ST_DATA:
            return
        if self.peer.mode == "lossy" and not self.peer.dropped_data:
            self.peer.dropped_data = True
            return
        if packet.seq_nr == (self.ack_nr + 1) & 0xFFFF:
            self.ack_nr = packet.seq_nr
            with self.condition:
                self.buffer.extend(packet.payload)
                self.condition.notify_all()
        self.peer.send(self.packet(ST_STATE, self.seq_nr), self.address)

    def recv(self, length):
        with self.condition:
            deadline = time.monotonic() + 3.0
            while not self.buffer and not self.closed and time.monotonic() < deadline:
                self.condition.wait(0.05)
            data = bytes(self.buffer[:length])
            del self.buffer[:length]
        return data

    def sendall(self, data):
        for start in range(0, len(data), self.peer.packet_size):
            seq_nr = self.seq_nr
            self.seq_nr = (self.seq_nr + 1) & 0xFFFF
            packet = self.packet(ST_DATA, seq_nr, data[start:start + self.peer.packet_size])
            self.unacked[seq_nr] = [packet, time.monotonic()]
            self.sent_count += 1
            if self.peer.mode != "reorder":
                self.peer.send(packet, self.address)
                continue
            self.pending_pair.append(packet)
            if self.sent_count % 3 == 0:
                self.peer.send(packet, self.address)
            if len(self.pending_pair) == 2:
                for queued in reversed(self.pending_pair):
                    self.peer.send(queued, self.address)
                self.pending_pair = []
        for queued in self.pending_pair:
            self.peer.send(queued, self.address)
        self.pending_pair = []

    def settimeout(self, value):
        return None

    def resend_due(self, now):
        for entry in list(self.unacked.values()):
            if now - entry[1] >= RESEND_SECONDS:
                entry[1] = now
                self.peer.send(entry[0], self.address)


class FakeUtpPeer:
    def __init__(self, info_hash, metadata, mode="ok", packet_size=1000):
        assert mode in MODES
        self.mode = mode
        self.packet_size = packet_size
        self.protocol = FakeMetadataPeer(info_hash, metadata, "ok", send_noise=False)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.settimeout(0.05)
        self.address = self.sock.getsockname()
        self.connections = {}
        self.dropped_syn = False
        self.dropped_data = False
        self.running = True
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.network_loop, name="fake-utp", daemon=True)
        self.thread.start()

    @property
    def requests(self):
        return self.protocol.requests

    def unacked_count(self):
        return sum(len(connection.unacked) for connection in self.connections.values())

    def send(self, packet, address):
        with self.lock:
            self.sock.sendto(packet, address)

    def close(self):
        self.running = False
        self.thread.join(3.0)
        self.sock.close()
        self.protocol.close()

    def network_loop(self):
        while self.running:
            try:
                data, address = self.sock.recvfrom(65535)
            except socket.timeout:
                data = None
            except OSError:
                break
            now = time.monotonic()
            for connection in list(self.connections.values()):
                connection.resend_due(now)
            if data is None:
                continue
            packet = decode_utp_packet(data)
            if packet is None:
                continue
            if packet.packet_type == ST_SYN:
                if self.mode == "lossy" and not self.dropped_syn:
                    self.dropped_syn = True
                    continue
                key = (address, (packet.connection_id + 1) & 0xFFFF)
                if key not in self.connections:
                    connection = FakeUtpConnection(self, address, packet)
                    self.connections[key] = connection
                    threading.Thread(target=self.serve, args=(connection,), name="fake-utp-session", daemon=True).start()
                connection = self.connections[key]
                self.send(connection.packet(ST_STATE, connection.seq_nr), address)
                continue
            connection = self.connections.get((address, packet.connection_id))
            if connection is not None:
                connection.on_packet(packet)

    def serve(self, connection):
        try:
            self.protocol.handle_connection(connection)
        except (OSError, ValueError, ConnectionError):
            pass
