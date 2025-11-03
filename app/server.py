import selectors
import types
from .parser import RESParser
from .db import DB, Value, ValueType
from .utils import QuickList
from .parser import RESPEncoder

def execute_cmd(args):
    output = b"$-1\r\n"
    args[0] = args[0].upper()
    if args[0] == "ECHO":
        output = b"+" + args[1].encode("utf-8") + b"\r\n" if len(args) > 1 else b"-ERR Too few args for ECHO\r\n"
    elif args[0] == "PING":
        output = b"+PONG\r\n"
    elif args[0] == "SET":
        if len(args) == 3:
            DB.set(args[1], Value(args[2], ValueType.STRING))
            output = b"+OK\r\n"
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
            length = DB.add_to_list(args[1], args[2:])
            if length is not None:
                output = RESPEncoder.encode_int(length)
            else:
                output = b"-ERR Key might not represent a list\r\n"
        else:
            output = b"-ERR RPUSH expects more than 2 args\r\n"
    elif args[0] == "LRANGE":
        if len(args) == 4:
            val = DB.get(args[1])
            arr = QuickList()
            if val is not None:
                if val.type_ == ValueType.LIST:
                    arr = val.val
                else:
                    return b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            n = arr.length
            st = None
            end = None
            try:
                st = int(args[2])
                end = int(args[3])
                if st < 0:
                    st = max(st + n, 0)
                if end < 0:
                    end += n
                end = min(end, n - 1)
                if 0 <= end < n and end >= st:
                    output = RESPEncoder.encode_arr(arr.lrange(st, end))
                else:
                    output = RESPEncoder.encode_arr([])
            except ValueError:
                output = b"-ERR range values not an integer\r\n"
        else:
            output = b"-ERR LRANGE expects 4 args\r\n"
    elif args[0] == "LLEN":
        if len(args) == 2:
            val = DB.get(args[1])
            if val is None:
                output = b":0\r\n"
            elif val.type_ == ValueType.LIST:
                output = RESPEncoder.encode_int(val.val.length)
            else:
                output = b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        else:
            output = b"-ERR LLEN only 1 arg\r\n"
    elif args[0] == "LPUSH":
        if len(args) >= 3:
            length = DB.add_to_list(args[1], args[2:], prepend=True)
            if length is not None:
                output = RESPEncoder.encode_int(length)
            else:
                output = b"-ERR Key might not represent a list\r\n"
        else:
            output = b"-ERR LPUSH expects more than 2 args\r\n"
    elif args[0] == "LPOP":
        if 2 <= len(args) <= 3:
            val = DB.get(args[1])
            if val is None:
                output = b"$-1\r\n"
            elif val.type_ != ValueType.LIST:
                output = b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            else:
                count = 1
                if len(args) == 3:
                    try:
                        count = min(int(args[2]), val.val.length)
                        if count < 0:
                            raise ValueError
                    except ValueError:
                        output = b"-ERR value out of range, must be positive\r\n"
                        return output
                arr = val.val.popleft(count)
                if not arr:
                    output = b"$-1\r\n"
                elif len(args) == 2:
                    output = RESPEncoder.encode_bulk_str(arr[0])
                else:
                    output = RESPEncoder.encode_arr(arr)
        else:
            output = b"-ERR LPOP expects 2 args\r\n"
    elif args[0] == "RPOP":
        pass
    elif args[0] == "DEL":
        if len(args) == 2:
            DB.delete(args[1])
            output = b"+OK\r\n"
        else:
            output = b"-ERR DEL expects 1 arg\r\n"
    else:
        output = b"-ERR unknown command\r\n"
    print(args, output)
    return output

sel = selectors.DefaultSelector()

def accept_connection(sock):
    conn, addr = sock.accept()
    print('Accepted', conn, 'from', addr)
    conn.setblocking(False)
    try:
        data = types.SimpleNamespace(parser=RESParser(), outb=b"", addr=addr)
        io_events = selectors.EVENT_READ | selectors.EVENT_WRITE
        sel.register(conn, io_events, data)
    except KeyError:
        print("File object", conn, "already registered")
    except ValueError:
        print("Invalid mask or fd in during register")

def service_connection(key, mask):
    conn = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = conn.recv(1024)
        if recv_data:
            args = data.parser.parse(recv_data)
            if args:
                data.outb += execute_cmd(args)
        else:
            print(f"Closing connection from {data.addr}")
            sel.unregister(conn)
            conn.close()
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            bytes_sent = conn.send(data.outb)
            data.outb = data.outb[bytes_sent:]

def event_loop():
    import socket
    import time
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    sel.register(server_socket, selectors.EVENT_READ, data=None)
    last_active_cleanup = time.time()
    max_wait = 0.1
    while True:
        events = sel.select(timeout=max_wait)
        for key, mask in events:
            if not key.data:
                accept_connection(key.fileobj)
            else:
                service_connection(key, mask)
        now = time.time()
        if now - last_active_cleanup > max_wait:
            DB.active_expire()
            last_active_cleanup = now
