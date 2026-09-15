import socket
import threading

import pytest


@pytest.fixture()
def hung_addr():
    """A server that accepts every connection and never answers.

    Models the outage the dead-port fixtures cannot: a partitioned or
    overloaded memcached whose socket still connects, so every command is
    written and then times out waiting for its response.
    """
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen()
    accepted: list[socket.socket] = []

    def accept() -> None:
        while True:
            try:
                connection, _ = server.accept()
            except OSError:
                return
            accepted.append(connection)

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    yield server.getsockname()
    try:
        server.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    server.close()
    for connection in accepted:
        connection.close()
    thread.join(1)
