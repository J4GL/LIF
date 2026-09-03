"""Helpers shared by several test categories."""
import http.client
import json
import time


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


def wait_until(condition, timeout=8.0, step=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(step)
    return condition()


def http_get(port, path, timeout=2.0):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    connection.request("GET", path)
    response = connection.getresponse()
    body = response.read()
    connection.close()
    return response, body


def http_get_json(port, path):
    response, body = http_get(port, path)
    return response.status, json.loads(body)
