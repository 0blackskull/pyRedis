import socket  # noqa: F401
import selectors
import types
from typing import List, Union
import time
import random

sel = selectors.DefaultSelector()

 # type : "str" "list" "set" "zset" "hash"
class ValueType:
    STRING = 0
    LIST = 1
    SET = 2
    ZSET = 3
    HASH = 4

class Value:
    def __init__(self, val: ValueType, type_: str):
        self.val = val
        self.type = type_

"""
In memory key-value database
"""
class DB:
    _store: dict[str, Value] = {}
    # List of (key, now + ttl)
    _expiries = []
    # key -> index in _expiries
    _expiry_index = {}

    def __init__(self):
        # Static class
        raise NotImplementedError("Attempted to instantiate static class")

    # Primary usecase is RPUSH
    # Returns new length on success, None on failure
    @classmethod
    def append_list(cls, key: str, items: list[str]):
        val = cls._store.get(key)
        length = None
        # Create new list
        if val is None:
            cls.set(key, Value(items, ValueType.LIST))
            length = len(items)
        # Append to existing list
        else:
            # Appending allowed on lists only
            if val.type != ValueType.LIST:
                return None

            val.val.extend(items)
            length = len(val.val)
        
        return length

    # Class level methods (not instance/self level)
    @classmethod
    def set(cls, key: str, val: Value, ttl = None):
        cls._store[key] = val

        if ttl: # ms
            idx = len(cls._expiries)
            cls._expiries.append((key, time.time() + ttl))
            cls._expiry_index[key] = idx

    @classmethod
    def get(cls, key: str):
        # Passive/Lazy expire
        idx = cls._expiry_index.get(key, None)

        if idx is not None:
            _, expiry = cls._expiries[idx]
            if expiry <= time.time():
                cls.delete(key)
                return None

        return cls._store.get(key, None)

    @classmethod
    def active_expire(cls, sample_size=100):
        if not cls._expiries:
            return

        random_idx = random.randrange(0, len(cls._expiries))
        now = time.time()
        to_delete = []

        for i in range(random_idx, min(random_idx + sample_size, len(cls._expiries))):

            key, expiry = cls._expiries[i]
            # Not expired
            if  now >= expiry:
                to_delete.append(key)
        
        for key in to_delete:
            cls.delete(key)

    @classmethod
    def delete(cls, key: str):
        cls._store.pop(key, None)

        idx = cls._expiry_index.pop(key, None)
        if idx is None:
            return

        last_key, _ = cls._expiries[-1]

        if idx != len(cls._expiries) -1:
            # Swap tuple at idx and end
            cls._expiries[idx], cls._expiries[-1] = cls._expiries[-1], cls._expiries[idx]
            # Update index of last key
            cls._expiry_index[last_key] = idx

        # Perform ttl tracking deletion
        cls._expiries.pop()

class RESPEncoder():
    def __init__():
        pass
    # static method, no need to pass 'self'
    @staticmethod
    def encode_simple_str(s: str) -> bytes:
        return f"+{s}\r\n".encode()
    
    @staticmethod
    def encode_bulk_str(s: str | None) -> bytes:
        if s is None:
            return b"$-1\r\n"
        return f"${len(s)}\r\n{s}\r\n".encode()
    
    @staticmethod
    def encode_int(i: int) -> bytes:
        return f":{i}\r\n".encode()
    
    @staticmethod
    def encode_err(msg: str) -> bytes:
        return f"-{msg}\r\n".encode()
    
    @staticmethod
    def encode_arr(items: list[str], st: int = 0, end: int = None) -> bytes:
        rend = len(items) - 1 if end is None else end
        encoded = b"*" + str(rend - st + 1).encode() + b"\r\n" 

        for i in range(st, rend + 1):
            encoded += f"${len(items[i])}\r\n{items[i]}\r\n".encode()

        return encoded

    @classmethod
    def encode_value(cls, val: Value):
        encoded = b"$-1\r\n"

        if val.type == ValueType.LIST:
            encoded = cls.encode_arr(val.val)
        elif val.type == ValueType.STRING:
            encoded = cls.encode_bulk_str(val.val)
        
        return encoded
 
"""
Decode incoming RESP payload (array of bulk strings)
returns array and remaining buffer
"""
class RESParser:
    def __init__(self) -> None:
        # type to arr_len to bulk_len to bulk_data
        self.state  = "type"
        self.pos = 0
        # binary received 
        self.buf = bytearray() # mutable byte array
        # processed
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
            # Prevent overrun
            if self.pos >= len(self.buf):
                break

            if self.state == "type":
                token = self.buf[self.pos : self.pos + 1]
                # Array start detected
                if token == b"*":
                    # Expect array length next
                    self.state = "arr_len"
                # Bulk string start detected
                elif token == b"$":
                    self.state = "bulk_len"
                else:
                    raise ValueError(f"Unsupported token found: {token!r}") # !r for raw string

                self.pos += 1

            elif self.state == "bulk_data":
                expected_end = self.pos + self.bulk_len + 2 # 2 for CRLF

                # Wait for buffer to reach correct size
                if len(self.buf) < expected_end:
                    break

                # Read and store arg
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
                
                # Expected bulk string length ready
                self.bulk_len = int(line.decode("utf-8"))
                self.state = "bulk_data"

            
            elif self.state == "arr_len":
                line = self._readline()
                if line is None:
                    break

                # Expected array length ready
                self.expected_args = int(line.decode("utf-8"))
                self.state = "type"

            # Interpreted / consumed all buffer
            if self.pos == len(self.buf):
                self.buf.clear()
                self.pos = 0

            # DEBUG statements
            # print(f"State {self.state} ; Pos {self.pos} ; Expected args {self.expected_args} ; Bulk len {self.bulk_len} ; Buffer {self.buf}")
            # print("args", self.args) # prints ["ECHO", "HELLO"]
        
        return results

"""
Executes commands received from clients after parsing
"""
def execute_cmd(args: List[str]):
    output = b"$-1\r\n"
    args[0] = args[0].upper()

    if args[0] == "ECHO":
        output = b"+" + args[1].encode("utf-8") + b"\r\n" if len(args) > 1 else b"-ERR Too few args for ECHO\r\n"

    elif args[0] == "PING":
        output = b"+PONG\r\n"

    elif args[0] == "SET":
        # Set without ttl
        if len(args) == 3:
            DB.set(args[1], Value(args[2], ValueType.STRING))
            output = b"+OK\r\n"
        # Set with expiry
        elif len(args) == 5:
            ttl = None
            args[3] = args[3].upper()

            if args[3] == "EX":
                ttl = int(args[4])
            elif args[3] == "PX":
                ttl = int(args[4]) / 1000

            if ttl is not None:
                DB.set(args[1], Value(args[2], ValueType.STRING), ttl)
                output = b"+OK\r\n"
            else:
                output = b"-ERR unknown arg\r\n"
        # Unknown case
        else:
            output = b"-ERR Incorrect number of args\r\n"

    elif args[0] == "GET":
        val = DB.get(args[1])

        if val is not None:
            output = RESPEncoder.encode_value(val)
        else:
            output = b"$-1\r\n"

    elif args[0] == "RPUSH":
        if len(args) >= 3:
            length = DB.append_list(args[1], args[2:])
            if length is not None:
                output = RESPEncoder.encode_int(length)
            else:
                output = b"-ERR Key might not represent a list\r\n"

        else:
            output = b"-ERR RPUSH expects more than 2 args\r\n"
    
    elif args[0] == "LRANGE":
        if len(args) == 4:
            val = DB.get(args[1])
            if val is not None:
                arr = val.val
                n = len(arr)
                try:
                    start = int(args[2])
                    if start < 0:
                        start %= n
                    end = int(args[3])
                    if end < 0:
                        end %= n
                except ValueError:
                    output = RESPEncoder.encode_error("Value not an integer")

                if start >= n or start > end:
                    output =  RESPEncoder.encode_arr([])
                else:
                    output = RESPEncoder.encode_arr(arr, start, min(end, len(arr) - 1))

        else:
            output = b"-ERR LRANGE expects 4 args\r\n"

    else:
        output = b"-ERR unknown command\r\n"
    
    # null type = $-1\r\n

    return output
            
# parser = RESParser()

# chunks = [
#     b"*2\r\n$4\r\nE",     # arrives first
#     b"CHO\r\n$5\r\nhe",   # second
#     b"llo\r\n"             # last
# ]

# for chunk in chunks:
#     parser.parse(chunk)

"""
Accept connection on a listening socket ready for read
"""
def accept_connection(sock):
    conn, addr = sock.accept()
    print('Accepted', conn, 'from', addr)
    conn.setblocking(False)
    
    try:
        # Single parser instance per connection 
        data = types.SimpleNamespace(parser=RESParser(), outb=b"", addr=addr)
        io_events = selectors.EVENT_READ | selectors.EVENT_WRITE
        sel.register(conn, io_events, data)
    except KeyError:
        print("File object", conn, "already registered")
    except ValueError:
        print("Invalid mask or fd in during register")


"""
Process event from a connection socket
"""
def service_connection(key: selectors.SelectorKey, mask: int):
    conn = key.fileobj
    data = key.data

    if mask & selectors.EVENT_READ:
        recv_data = conn.recv(1024)

        # Storing data received from client
        if recv_data:
            args = data.parser.parse(recv_data)
            if args:
                data.outb += execute_cmd(args)

        else:
            print(f"Closing connection from {data.addr}")
            # Cleanup
            sel.unregister(conn)
            conn.close()
    
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            bytes_sent = conn.send(data.outb)
            data.outb = data.outb[bytes_sent:]

def main():
    # bind and listen
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    sel.register(server_socket, selectors.EVENT_READ, data=None)

    last_active_cleanup = time.time()
    max_wait = 0.1 # 100ms

    """
    Event Loop setup
    """
    while True:
        # Wait for next message 
        events = sel.select(timeout=max_wait)
        
        # Process message
        for key, mask in events:
            # Message from new connection
            if not key.data:
                accept_connection(key.fileobj)
            else:
                service_connection(key, mask)
        
        # Throttle active cleanup in case IO arrives earlier than 100 ms
        now = time.time()
        if now - last_active_cleanup > max_wait:
            DB.active_expire()
            last_active_cleanup = now


if __name__ == "__main__":
    main()
