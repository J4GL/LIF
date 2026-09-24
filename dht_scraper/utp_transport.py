"""uTP (BEP 29) client: outgoing connections multiplexed on one asyncio UDP socket of the fetch engine."""
import asyncio
import os
import struct
import time
from typing import Dict, List, NamedTuple, Optional, Tuple

from dht_scraper.krpc_messages import Address
from dht_scraper.metadata_fetcher import DEFAULT_TIMEOUTS, FetchTimeouts, MetadataFetchError, run_session, verify_metadata, within
from dht_scraper.node_identity import NODE_ID_LENGTH

ST_DATA = 0
ST_FIN = 1
ST_STATE = 2
ST_RESET = 3
ST_SYN = 4
UTP_VERSION = 1
HEADER = struct.Struct("!BBHIIIHH")
SEQUENCE_MASK = 0xFFFF
TIMESTAMP_MASK = 0xFFFFFFFF
MAX_PAYLOAD = 1200
RECEIVE_WINDOW = 1 << 20
MAX_OUT_OF_ORDER = 1024
INITIAL_RESEND_SECONDS = 1.0
MAX_RESENDS = 4
RESEND_CHECK_SECONDS = 0.25


class UtpPacket(NamedTuple):
    packet_type: int
    connection_id: int
    timestamp: int
    timestamp_difference: int
    window: int
    seq_nr: int
    ack_nr: int
    payload: bytes


# Parents: UtpConnection.send_packet, tests
# Keywords: utp, header, encode, big endian
def encode_utp_packet(packet_type: int, connection_id: int, timestamp: int, timestamp_difference: int, window: int, seq_nr: int, ack_nr: int, payload: bytes = b"") -> bytes:
    assert ST_DATA <= packet_type <= ST_SYN and 0 <= connection_id <= SEQUENCE_MASK
    header = HEADER.pack((packet_type << 4) | UTP_VERSION, 0, connection_id, timestamp & TIMESTAMP_MASK, timestamp_difference & TIMESTAMP_MASK, window, seq_nr & SEQUENCE_MASK, ack_nr & SEQUENCE_MASK)
    result = header + payload
    assert len(result) == HEADER.size + len(payload)
    return result


# Parents: UtpSocket.datagram_received, tests
# Keywords: utp, header, decode, extensions, validation
def decode_utp_packet(data: bytes) -> Optional[UtpPacket]:
    assert isinstance(data, bytes)
    if len(data) < HEADER.size:
        return None
    type_version, extension, connection_id, timestamp, difference, window, seq_nr, ack_nr = HEADER.unpack_from(data)
    if type_version & 0x0F != UTP_VERSION or type_version >> 4 > ST_SYN:
        return None
    offset = HEADER.size
    while extension:
        if offset + 2 > len(data) or offset + 2 + data[offset + 1] > len(data):
            return None
        extension, offset = data[offset], offset + 2 + data[offset + 1]
    result = UtpPacket(type_version >> 4, connection_id, timestamp, difference, window, seq_nr, ack_nr, data[offset:])
    assert result.packet_type <= ST_SYN
    return result


# Parents: UtpConnection.acknowledge, UtpConnection.data_received
# Keywords: sequence number, wrap around, comparison
def sequence_after(later: int, earlier: int) -> bool:
    assert 0 <= later <= SEQUENCE_MASK and 0 <= earlier <= SEQUENCE_MASK
    result = later != earlier and ((later - earlier) & SEQUENCE_MASK) < 0x8000
    assert isinstance(result, bool)
    return result


# Parents: UtpConnection.send_packet, UtpConnection.packet_received
# Keywords: microseconds, timestamp, clock
def microseconds() -> int:
    result = int(time.monotonic() * 1_000_000) & TIMESTAMP_MASK
    assert 0 <= result <= TIMESTAMP_MASK
    return result


class UtpConnection:
    """One outgoing uTP connection; offers the ByteStream methods used by run_session."""

    # Parents: UtpSocket.connect
    # Keywords: connection ids, sequence numbers, state
    def __init__(self, utp_socket: "UtpSocket", address: Address, recv_id: int) -> None:
        assert 0 <= recv_id <= SEQUENCE_MASK
        loop = asyncio.get_running_loop()
        self.socket = utp_socket
        self.address = address
        self.recv_id = recv_id
        self.send_id = (recv_id + 1) & SEQUENCE_MASK
        self.seq_nr = 1
        self.ack_nr = 0
        self.connected_state = False
        self.closed = False
        self.error: Optional[str] = None
        self.reply_micro = 0
        self.buffer = bytearray()
        self.received_any = False
        self.out_of_order: Dict[int, bytes] = {}
        self.eof_seq: Optional[int] = None
        self.unacked: Dict[int, List] = {}
        self.connected = loop.create_future()
        self.readable = asyncio.Event()
        self.ack_scheduled = False
        self.resend_timer: Optional[asyncio.TimerHandle] = None
        assert not self.closed

    # Parents: send_syn, write, send_ack, close
    # Keywords: packet, send, timestamps, window
    def send_packet(self, packet_type: int, seq_nr: int, payload: bytes = b"") -> bytes:
        assert not self.closed or packet_type == ST_RESET
        connection_id = self.recv_id if packet_type == ST_SYN else self.send_id
        window = max(0, RECEIVE_WINDOW - len(self.buffer))
        packet = encode_utp_packet(packet_type, connection_id, microseconds(), self.reply_micro, window, seq_nr, self.ack_nr, payload)
        self.socket.send(packet, self.address)
        assert len(packet) >= HEADER.size
        return packet

    # Parents: UtpSocket.connect
    # Keywords: syn, resend, first packet
    def send_syn(self) -> None:
        assert not self.connected_state
        self.unacked[self.seq_nr] = [self.send_packet(ST_SYN, self.seq_nr), time.monotonic(), 0]
        self.seq_nr = (self.seq_nr + 1) & SEQUENCE_MASK
        self.arm_resend()
        assert self.unacked

    # Parents: run_session (ByteStream)
    # Keywords: write, data packets, unacked
    def write(self, data: bytes) -> None:
        assert isinstance(data, bytes) and data
        if self.closed:
            return
        for start in range(0, len(data), MAX_PAYLOAD):
            packet = self.send_packet(ST_DATA, self.seq_nr, data[start:start + MAX_PAYLOAD])
            self.unacked[self.seq_nr] = [packet, time.monotonic(), 0]
            self.seq_nr = (self.seq_nr + 1) & SEQUENCE_MASK
        self.arm_resend()
        assert self.unacked

    # Parents: UtpSocket.datagram_received
    # Keywords: receive, syn ack, ack, data, fin, reset
    def packet_received(self, packet: UtpPacket) -> None:
        assert isinstance(packet, UtpPacket)
        if self.closed:
            return
        self.reply_micro = (microseconds() - packet.timestamp) & TIMESTAMP_MASK
        if packet.packet_type == ST_RESET:
            self.fail("closed")
            return
        if not self.connected_state:
            if packet.packet_type != ST_STATE:
                return
            self.ack_nr = (packet.seq_nr - 1) & SEQUENCE_MASK
            self.connected_state = True
            self.acknowledge(packet.ack_nr)
            if not self.connected.done():
                self.connected.set_result(True)
            return
        self.acknowledge(packet.ack_nr)
        if packet.packet_type == ST_DATA:
            self.data_received(packet.seq_nr, packet.payload)
        elif packet.packet_type == ST_FIN:
            self.eof_seq = packet.seq_nr
            self.data_received(packet.seq_nr, b"")
        assert self.connected_state

    # Parents: packet_received
    # Keywords: ack, unacked, cumulative
    def acknowledge(self, ack_nr: int) -> None:
        assert 0 <= ack_nr <= SEQUENCE_MASK
        for seq_nr in [seq_nr for seq_nr in self.unacked if not sequence_after(seq_nr, ack_nr)]:
            del self.unacked[seq_nr]
        assert all(sequence_after(seq_nr, ack_nr) for seq_nr in self.unacked)

    # Parents: packet_received
    # Keywords: in order delivery, out of order buffer, duplicates
    def data_received(self, seq_nr: int, payload: bytes) -> None:
        assert 0 <= seq_nr <= SEQUENCE_MASK
        if seq_nr == (self.ack_nr + 1) & SEQUENCE_MASK:
            self.deliver(seq_nr, payload)
            following = (self.ack_nr + 1) & SEQUENCE_MASK
            while following in self.out_of_order:
                self.deliver(following, self.out_of_order.pop(following))
                following = (self.ack_nr + 1) & SEQUENCE_MASK
        elif sequence_after(seq_nr, self.ack_nr) and len(self.out_of_order) < MAX_OUT_OF_ORDER:
            self.out_of_order[seq_nr] = payload
        self.schedule_ack()
        assert (self.ack_nr + 1) & SEQUENCE_MASK not in self.out_of_order

    # Parents: data_received
    # Keywords: deliver, buffer, readable
    def deliver(self, seq_nr: int, payload: bytes) -> None:
        assert seq_nr == (self.ack_nr + 1) & SEQUENCE_MASK
        self.ack_nr = seq_nr
        if payload:
            self.buffer.extend(payload)
            self.received_any = True
        self.readable.set()
        assert self.ack_nr == seq_nr

    # Parents: data_received, packet_received
    # Keywords: ack, coalesce, loop pass
    def schedule_ack(self) -> None:
        assert self.connected_state
        if not self.ack_scheduled:
            self.ack_scheduled = True
            asyncio.get_running_loop().call_soon(self.send_ack)

    # Parents: schedule_ack (callback)
    # Keywords: st_state, ack, no sequence consumed
    def send_ack(self) -> None:
        self.ack_scheduled = False
        if not self.closed:
            self.send_packet(ST_STATE, self.seq_nr)
        assert not self.ack_scheduled

    # Parents: send_syn, write, check_resend
    # Keywords: resend timer, arm
    def arm_resend(self) -> None:
        assert self.unacked or self.closed
        if self.resend_timer is None and not self.closed:
            self.resend_timer = asyncio.get_running_loop().call_later(RESEND_CHECK_SECONDS, self.check_resend)

    # Parents: arm_resend (timer callback)
    # Keywords: resend, doubling timeout, give up
    def check_resend(self) -> None:
        self.resend_timer = None
        if self.closed:
            return
        now = time.monotonic()
        for entry in self.unacked.values():
            if now - entry[1] < INITIAL_RESEND_SECONDS * (2 ** entry[2]):
                continue
            if entry[2] >= MAX_RESENDS:
                self.fail("connect_timeout" if not self.connected_state else "timeout")
                return
            entry[1], entry[2] = now, entry[2] + 1
            self.socket.send(entry[0], self.address)
        if self.unacked:
            self.arm_resend()
        assert not self.closed

    # Parents: packet_received, check_resend
    # Keywords: failure, wake readers
    def fail(self, reason: str) -> None:
        assert reason in ("closed", "timeout", "connect_timeout")
        self.error = self.error or reason
        if not self.connected.done():
            self.connected.set_result(False)
        self.readable.set()
        self.close(send_reset=False)
        assert self.closed

    # Parents: run_session (ByteStream)
    # Keywords: read exact, deadline, eof, closed on handshake
    async def read_exactly(self, length: int, deadline: float, closed_reason: str = "closed") -> bytes:
        assert length > 0
        loop = asyncio.get_running_loop()
        while len(self.buffer) < length:
            ended = self.error is not None or (self.eof_seq is not None and self.ack_nr == self.eof_seq)
            if ended:
                raise MetadataFetchError("timeout" if self.error == "timeout" else (closed_reason if not self.received_any else "closed"))
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise MetadataFetchError("timeout")
            self.readable.clear()
            try:
                await within(self.readable.wait(), remaining)
            except asyncio.TimeoutError:
                raise MetadataFetchError("timeout")
        result = bytes(self.buffer[:length])
        del self.buffer[:length]
        assert len(result) == length
        return result

    # Parents: fetch_metadata_utp, fail, UtpSocket.connect
    # Keywords: close, reset, forget
    def close(self, send_reset: bool = True) -> None:
        if self.closed:
            return
        if send_reset and self.connected_state:
            self.send_packet(ST_STATE, self.seq_nr)
            self.send_packet(ST_RESET, self.seq_nr)
        self.closed = True
        if self.resend_timer is not None:
            self.resend_timer.cancel()
            self.resend_timer = None
        self.socket.forget(self)
        assert self.closed


class UtpSocket(asyncio.DatagramProtocol):
    """One UDP socket for all outgoing uTP connections, dispatching packets by address and connection id."""

    # Parents: open_utp_socket
    # Keywords: multiplexer, connections table
    def __init__(self) -> None:
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.connections: Dict[Tuple[str, int, int], UtpConnection] = {}
        self.packets_sent = 0
        assert not self.connections

    # Parents: asyncio (endpoint created)
    # Keywords: transport
    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert transport is not None
        self.transport = transport  # type: ignore

    # Parents: asyncio (datagram)
    # Keywords: dispatch, connection id
    def datagram_received(self, data: bytes, address: Tuple) -> None:
        assert isinstance(data, bytes)
        packet = decode_utp_packet(data)
        if packet is not None:
            connection = self.connections.get((address[0], address[1], packet.connection_id))
            if connection is not None:
                connection.packet_received(packet)

    # Parents: asyncio (icmp errors)
    # Keywords: ignore, unreachable
    def error_received(self, exc: Exception) -> None:
        assert exc is not None

    # Parents: UtpConnection.send_packet, UtpConnection.check_resend
    # Keywords: sendto, counter
    def send(self, packet: bytes, address: Address) -> None:
        assert isinstance(packet, bytes)
        if self.transport is not None and not self.transport.is_closing():
            self.transport.sendto(packet, address)
            self.packets_sent += 1

    # Parents: fetch_metadata_utp
    # Keywords: connect, syn, timeout
    async def connect(self, address: Address, timeout: float) -> UtpConnection:
        assert len(address) == 2 and timeout > 0
        recv_id = int.from_bytes(os.urandom(2), "big")
        while (address[0], address[1], recv_id) in self.connections:
            recv_id = (recv_id + 1) & SEQUENCE_MASK
        connection = UtpConnection(self, address, recv_id)
        self.connections[(address[0], address[1], recv_id)] = connection
        connection.send_syn()
        try:
            connected = await within(asyncio.shield(connection.connected), timeout)
        except asyncio.TimeoutError:
            connection.close(send_reset=False)
            raise MetadataFetchError("connect_timeout")
        except BaseException:
            connection.close(send_reset=False)
            raise
        if not connected:
            raise MetadataFetchError(connection.error if connection.error in ("closed", "connect_timeout") else "connect")
        assert connection.connected_state
        return connection

    # Parents: UtpConnection.close
    # Keywords: forget, table cleanup
    def forget(self, connection: UtpConnection) -> None:
        assert isinstance(connection, UtpConnection)
        self.connections.pop((connection.address[0], connection.address[1], connection.recv_id), None)

    # Parents: FetchEngine.main, fetch_metadata_utp
    # Keywords: close, transport
    def close(self) -> None:
        for connection in list(self.connections.values()):
            connection.close()
        if self.transport is not None:
            self.transport.close()
        assert not self.connections


# Parents: FetchEngine.main, fetch_metadata_utp
# Keywords: udp endpoint, bind, any port
async def open_utp_socket() -> UtpSocket:
    loop = asyncio.get_running_loop()
    _, protocol = await loop.create_datagram_endpoint(UtpSocket, local_addr=("0.0.0.0", 0))
    assert isinstance(protocol, UtpSocket)
    return protocol


# Parents: FetchEngine.attempt (uTP function), tests
# Keywords: fetch, utp, session, verified metadata
async def fetch_metadata_utp(info_hash: bytes, address: Address, utp_socket: Optional[UtpSocket] = None, timeouts: FetchTimeouts = DEFAULT_TIMEOUTS) -> bytes:
    assert len(info_hash) == NODE_ID_LENGTH and len(address) == 2
    owned = utp_socket is None
    utp_socket = utp_socket or await open_utp_socket()
    loop = asyncio.get_running_loop()
    session_deadline = loop.time() + timeouts.session
    try:
        connection = await utp_socket.connect(address, timeouts.utp_connect)
        try:
            metadata = await run_session(connection, info_hash, timeouts, session_deadline)
        finally:
            connection.close()
    finally:
        if owned:
            utp_socket.close()
    verify_metadata(info_hash, metadata)
    assert len(metadata) > 0
    return metadata
