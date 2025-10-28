import socket  # noqa: F401
import selectors
import types

sel = selectors.DefaultSelector()

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
        self.bulk_data = b""

    def _readline(self):
        end_idx = self.buf.find(b"\r\n")
        if end_idx < 0:
            return None
        
        line = self.buf[self.pos : end_idx]
        self.pos = end_idx + 2
        return line

    def parse(self, data):
        self.buf += data

        while True:
            # Prevent overrun
            if self.pos >= len(self.buf):
                break

            token = self.buf[self.pos : self.pos + 1]

            # if token == b"\n":
            #     self.pos += 1
            #     continue

            if self.state == "type":
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

                if len(self.bulk_data) == self.bulk_len:
                    if token != b"\r" and token != b"\n":
                        raise ValueError(f"Malformed payload: {self.bulk_data!r}")
                    
                    self.args.append(self.bulk_data.decode('utf-8'))

                    # Ready for next unit
                    self.state = "type"
                    self.bulk_len = 0
                    self.bulk_data = b""
                    
                else:
                    self.bulk_data += token
                
                self.pos += 1


            elif self.state == "bulk_len":
                # Bulk string length reading complete
                if token == b"\r" or token == b"\n":
                    self.state = "bulk_data"
                # Update expected number of args
                else:
                    self.bulk_len = self.bulk_len * 10 + int(token.decode())

                self.pos += 1

            
            elif self.state == "arr_len":
                
                # Array length (number of args) reading complete
                if token == b"\r":
                    self.state = "type"
                # Update expected number of args
                else:
                    self.expected_args = self.expected_args * 10 + int(token.decode())
                    print(int(token.decode()))

                self.pos += 1

            print(f"Token {token} ; State {self.state} ; Pos {self.pos} ; Expected args {self.expected_args} ; Bulk len {self.bulk_len}")
            print("args", self.args) # prints ["ECHO", "HELLO"]


            
parser = RESParser()

chunks = [
    b"*2\r\n$4\r\nE",     # arrives first
    b"CHO\r\n$5\r\nhe",   # second
    b"llo\r\n"             # last
]

for chunk in chunks:
    parser.parse(chunk)
    



"""
Accept connection on a listening socket ready for read
"""
def accept_connection(sock):
    conn, addr = sock.accept()
    print('Accepted', conn, 'from', addr)
    conn.setblocking(False)
    
    try:
        data = types.SimpleNamespace(parser=RESParser, outb=b"", addr=addr)
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
            data.outb += recv_data
        else:
            print(f"Closing connection from {data.addr}")
            # Cleanup
            sel.unregister(conn)
            conn.close()
    
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            print("Responding with PONG")
            bytes_sent = conn.send(b"+PONG\r\n")
            # data.outb = data.outb[bytes_sent:]
            data.outb = b""

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


# if __name__ == "__main__":
#     main()
