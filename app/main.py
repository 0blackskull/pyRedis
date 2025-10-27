import socket  # noqa: F401
import selectors
import types

sel = selectors.DefaultSelector()

"""
Accept connection on a listening socket ready for read
"""
def accept_connection(sock):
    conn, addr = sock.accept()
    print('Accepted', conn, 'from', addr)
    conn.setblocking(False)
    
    try:
        data = types.SimpleNamespace(inb="", outb="", addr=addr)
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
        print("Responding with PONG")
        conn.send("+PONG\r\n")



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
