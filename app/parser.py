class RESPEncoder():
    def __init__():
        pass
    # static method, no need to pass 'self'
    @staticmethod
    def encode_simple_str(s: str) -> bytes:
        return f"+{s}\r\n".encode()
    
    @staticmethod
    def encode_bulk_str(s):
        if s is None:
            return b"$-1\r\n"
        return f"${len(s)}\r\n{s}\r\n".encode()
    
    @staticmethod
    def encode_int(i):
        return f":{i}\r\n".encode()
    
    @staticmethod
    def encode_err(msg: str) -> bytes:
        return f"-{msg}\r\n".encode()
    
    @staticmethod
    def encode_arr(items):
        return b"*" + str(len(items)).encode() + b"\r\n" + (
            b"".join(f"${len(item)}\r\n{item}\r\n".encode() for item in items)
        )

    @classmethod
    def encode_value(cls, val):
        from .db import ValueType
        encoded = b"$-1\r\n"
        if val.type_ == ValueType.LIST:
            encoded = cls.encode_arr(val.val)
        elif val.type_ == ValueType.STRING:
            encoded = cls.encode_bulk_str(val.val)
        return encoded

class RESParser:
    def __init__(self) -> None:
        self.state  = "type"
        self.pos = 0
        self.buf = bytearray()
        self.args = []
        self.expected_args = 0
        self.bulk_len = 0

    def _readline(self):
        end_idx = self.buf.find(b"\r\n", self.pos)
        if end_idx < 0:
            return None
        line = self.buf[self.pos : end_idx]
        self.pos = end_idx + 2
        return line

    def parse(self, data):
        results = []
        self.buf += data
        while True:
            if self.pos >= len(self.buf):
                break
            if self.state == "type":
                token = self.buf[self.pos : self.pos + 1]
                if token == b"*":
                    self.state = "arr_len"
                elif token == b"$":
                    self.state = "bulk_len"
                else:
                    raise ValueError(f"Unsupported token found: {token!r}")
                self.pos += 1
            elif self.state == "bulk_data":
                expected_end = self.pos + self.bulk_len + 2
                if len(self.buf) < expected_end:
                    break
                arg = self.buf[self.pos : self.pos + self.bulk_len]
                self.args.append(arg.decode('utf-8'))
                self.state = "type"
                self.pos = expected_end
                self.bulk_len = 0
                if len(self.args) == self.expected_args:
                    print(self.args)
                    results = self.args
                    self.args = []
                    self.expected_args = 0
            elif self.state == "bulk_len":
                line = self._readline()
                if line is None:
                    break
                self.bulk_len = int(line.decode("utf-8"))
                self.state = "bulk_data"
            elif self.state == "arr_len":
                line = self._readline()
                if line is None:
                    break
                self.expected_args = int(line.decode("utf-8"))
                self.state = "type"
            if self.pos == len(self.buf):
                self.buf.clear()
                self.pos = 0
        return results
