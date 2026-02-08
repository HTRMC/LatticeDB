"""GP Protocol wire format encoder/decoder.

Frame: [type:1B][length:4B BE][payload:NB]

Client -> Server:
  query        = 0x01  (payload = SQL string UTF-8)
  disconnect   = 0x02  (payload = empty)

Server -> Client:
  ok_message   = 0x10  (payload = message UTF-8)
  ok_row_count = 0x11  (payload = u64 BE)
  result_set   = 0x12  (payload = col_count:u16 + col_names + row_count:u32 + values)
  error_response = 0x1F (payload = error message UTF-8)
"""

import struct
from dataclasses import dataclass, field

# Message type constants
MSG_QUERY = 0x01
MSG_DISCONNECT = 0x02
MSG_OK_MESSAGE = 0x10
MSG_OK_ROW_COUNT = 0x11
MSG_RESULT_SET = 0x12
MSG_ERROR_RESPONSE = 0x1F

HEADER_SIZE = 5
MAX_PAYLOAD_SIZE = 16 * 1024 * 1024

_VALID_TYPES = {MSG_QUERY, MSG_DISCONNECT, MSG_OK_MESSAGE, MSG_OK_ROW_COUNT, MSG_RESULT_SET, MSG_ERROR_RESPONSE}


@dataclass
class ResultSet:
    columns: list[str]
    rows: list[list[str]]


@dataclass
class Message:
    msg_type: int
    payload: bytes

    @property
    def is_error(self) -> bool:
        return self.msg_type == MSG_ERROR_RESPONSE

    @property
    def text(self) -> str:
        """Decode payload as UTF-8 text (for ok_message, error_response, query)."""
        return self.payload.decode("utf-8")

    @property
    def row_count(self) -> int:
        """Decode payload as u64 BE (for ok_row_count)."""
        return struct.unpack(">Q", self.payload)[0]

    def result_set(self) -> ResultSet:
        """Parse payload as RESULT_SET."""
        return parse_result_set(self.payload)


def encode_query(sql: str) -> bytes:
    """Build a QUERY frame."""
    payload = sql.encode("utf-8")
    return _encode(MSG_QUERY, payload)


def encode_disconnect() -> bytes:
    """Build a DISCONNECT frame."""
    return _encode(MSG_DISCONNECT, b"")


def decode_message(data: bytes) -> Message:
    """Parse a response frame from raw bytes."""
    if len(data) < HEADER_SIZE:
        raise ValueError(f"incomplete header: got {len(data)} bytes, need {HEADER_SIZE}")

    msg_type = data[0]
    if msg_type not in _VALID_TYPES:
        raise ValueError(f"invalid message type: 0x{msg_type:02x}")

    payload_len = struct.unpack(">I", data[1:5])[0]
    if payload_len > MAX_PAYLOAD_SIZE:
        raise ValueError(f"payload too large: {payload_len}")
    if len(data) < HEADER_SIZE + payload_len:
        raise ValueError(f"incomplete payload: got {len(data) - HEADER_SIZE}, need {payload_len}")

    return Message(msg_type=msg_type, payload=data[HEADER_SIZE:HEADER_SIZE + payload_len])


def parse_result_set(payload: bytes) -> ResultSet:
    """Parse a RESULT_SET payload into columns and rows."""
    pos = 0

    if len(payload) < 2:
        raise ValueError("result set too short for column count")
    col_count = struct.unpack(">H", payload[pos:pos + 2])[0]
    pos += 2

    columns = []
    for _ in range(col_count):
        if pos + 2 > len(payload):
            raise ValueError("result set truncated in column names")
        name_len = struct.unpack(">H", payload[pos:pos + 2])[0]
        pos += 2
        if pos + name_len > len(payload):
            raise ValueError("result set truncated in column name data")
        columns.append(payload[pos:pos + name_len].decode("utf-8"))
        pos += name_len

    if pos + 4 > len(payload):
        raise ValueError("result set truncated before row count")
    row_count = struct.unpack(">I", payload[pos:pos + 4])[0]
    pos += 4

    rows = []
    for _ in range(row_count):
        row = []
        for _ in range(col_count):
            if pos + 2 > len(payload):
                raise ValueError("result set truncated in row values")
            val_len = struct.unpack(">H", payload[pos:pos + 2])[0]
            pos += 2
            if pos + val_len > len(payload):
                raise ValueError("result set truncated in value data")
            row.append(payload[pos:pos + val_len].decode("utf-8"))
            pos += val_len
        rows.append(row)

    return ResultSet(columns=columns, rows=rows)


def _encode(msg_type: int, payload: bytes) -> bytes:
    """Build a framed GP message: [type:1B][length:4B BE][payload:NB]."""
    return struct.pack(">BI", msg_type, len(payload)) + payload
