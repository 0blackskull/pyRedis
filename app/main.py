import socket  # noqa: F401
import selectors
import types
from typing import List

sel = selectors.DefaultSelector()

"""
In memory key-value database
"""
class DB:
    _store = {}

    def __init__(self):
        # Static class
        raise NotImplementedError("Static class")

    # Class level methods (not instance/self level)
    @classmethod
    def set(cls, key: str, val: str):
        cls._store[key] = val

    @classmethod
    def get(cls, key: str):
        return cls._store.get(key, None)

    

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
    output = b""

    # Supporting only echo for now
    if args[0] == "ECHO":
        output = b"+" + args[1].encode("utf-8") + b"\r\n" if len(args) > 1 else b"-ERR Too few args for ECHO\r\n"

    elif args[0] == "PING":
        output = b"+PONG\r\n"

    elif args[0] == "SET":
        if len(args) < 3:
            output = b"-ERR Too few args for ECHO\r\n"
        else:
            DB.set(args[1], args[2])
            output = b"+OK\r\n"

    elif args[0] == "GET":
        val = DB.get(args[1])
        output = b"$-1\r\n" if val is None else b"+" + val.encode("utf-8") + b"\r\n"
      
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
            print("Responding with PONG")
            bytes_sent = conn.send(data.outb)
            data.outb = data.outb[bytes_sent:]

def main():
    # bind and listen
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    sel.register(server_socket, selectors.EVENT_READ, data=None)

    """
    Event Loop setup
    """
    while True:
        # Wait for next message (blocking)
        events = sel.select(timeout=None)

        # Process message
        for key, mask in events:
            # Message from new connection
            if not key.data:
                accept_connection(key.fileobj)
            else:
                service_connection(key, mask)


if __name__ == "__main__":
    main()
