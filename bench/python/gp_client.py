"""Async QUIC client for GrapheneDB using aioquic."""

import asyncio
import ssl

from aioquic.asyncio import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived, StreamReset

from gp_protocol import Message, decode_message, encode_disconnect, encode_query


class _GPProtocol(QuicConnectionProtocol):
    """Custom protocol that collects stream data without creating StreamReader/Writer."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._responses: dict[int, bytearray] = {}
        self._waiters: dict[int, asyncio.Future] = {}

    def quic_event_received(self, event) -> None:
        if isinstance(event, StreamDataReceived):
            sid = event.stream_id
            if sid not in self._responses:
                self._responses[sid] = bytearray()
            self._responses[sid].extend(event.data)
            if event.end_stream:
                data = bytes(self._responses.pop(sid))
                waiter = self._waiters.pop(sid, None)
                if waiter and not waiter.done():
                    waiter.set_result(data)
        elif isinstance(event, StreamReset):
            sid = event.stream_id
            self._responses.pop(sid, None)
            waiter = self._waiters.pop(sid, None)
            if waiter and not waiter.done():
                waiter.set_exception(ConnectionError(f"stream reset: {event.error_code}"))

    async def send_and_recv(self, frame: bytes, timeout: float = 30.0) -> bytes:
        stream_id = self._quic.get_next_available_stream_id()
        waiter = asyncio.get_running_loop().create_future()
        self._waiters[stream_id] = waiter
        self._quic.send_stream_data(stream_id, frame, end_stream=True)
        self.transmit()
        return await asyncio.wait_for(waiter, timeout=timeout)


class GrapheneDBClient:
    """QUIC client that speaks the GP protocol.

    Each query opens a new bidirectional stream on the same QUIC connection.
    """

    def __init__(self):
        self._protocol: _GPProtocol | None = None
        self._connection = None

    async def connect(self, host: str = "127.0.0.1", port: int = 4321) -> None:
        """Establish a QUIC connection with ALPN 'graphenedb', skip cert validation."""
        config = QuicConfiguration(
            is_client=True,
            alpn_protocols=["graphenedb"],
        )
        config.verify_mode = ssl.CERT_NONE

        self._connection = connect(
            host,
            port,
            configuration=config,
            create_protocol=_GPProtocol,
        )
        self._protocol = await self._connection.__aenter__()

    async def query(self, sql: str) -> Message:
        """Send a SQL query and return the server response."""
        if self._protocol is None:
            raise RuntimeError("not connected")
        data = await self._protocol.send_and_recv(encode_query(sql))
        return decode_message(data)

    async def disconnect(self) -> None:
        """Send DISCONNECT and close the QUIC connection."""
        if self._protocol is None:
            return

        try:
            await self._protocol.send_and_recv(encode_disconnect())
        except Exception:
            pass

        try:
            await self._connection.__aexit__(None, None, None)
        except Exception:
            pass
        self._protocol = None
        self._connection = None
