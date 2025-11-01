import socket
import subprocess
import time
import pytest
import os

HOST = "127.0.0.1"
PORT = 6379


def send_command(cmd):
    with socket.create_connection((HOST, PORT)) as s:
        parts = cmd.split()
        payload = f"*{len(parts)}\r\n" + "".join(f"${len(p)}\r\n{p}\r\n" for p in parts)
        s.sendall(payload.encode())
        return s.recv(4096).decode()

def send_commands(cmds):
    with socket.create_connection((HOST, PORT)) as s:
        for cmd in cmds:
            parts = cmd.split()
            payload = f"*{len(parts)}\r\n" + "".join(f"${len(p)}\r\n{p}\r\n" for p in parts)
            s.sendall(payload.encode())
            print(s.recv(4096).decode())


@pytest.fixture(scope="module", autouse=True)
def server():
    proc = subprocess.Popen(["python", "server.py"])
    time.sleep(0.5)
    yield
    proc.terminate()
    proc.wait(timeout=2)


def test_1_rpush_create_and_append():
    send_command("SET normalkey hello")
    res = send_command("RPUSH mylist a")
    assert res.startswith(":1"), f"Expected :1, got {res}"

    res = send_command("RPUSH mylist b")
    assert res.startswith(":2")

    res = send_command("RPUSH mylist c d")
    assert res.startswith(":4")

    err = send_command("RPUSH normalkey z")
    assert err.startswith("-ERR") or "wrong type" in err.lower(), f"Expected error, got {err}"

    # cleanup
    send_command("DEL mylist")
    send_command("DEL normalkey")


def test_2_lrange_boundaries_and_errors():
    send_command("RPUSH mylist a b c d")
    send_command("SET normalkey hello")

    res = send_command("LRANGE mylist 0 1")
    assert "*2" in res, f"Expected 2 elements, got {res}"

    res = send_command("LRANGE mylist 10 20")
    assert res.startswith("*0"), f"Expected empty, got {res}"

    res = send_command("LRANGE mylist -100 -50")
    assert res.startswith("*0")

    err = send_command("LRANGE normalkey 0 1")
    assert err.startswith("-ERR") or "wrong type" in err.lower()

    # cleanup
    send_command("DEL mylist")
    send_command("DEL normalkey")


def test_3_lpush_and_boundaries():
    send_command("RPUSH mylist a b c d")
    send_command("SET normalkey hello")

    res = send_command("LPUSH mylist x")
    assert res.startswith(":5"), f"Expected :5, got {res}"

    res = send_command("LPUSH mylist y z")
    assert res.startswith(":7")

    err = send_command("LPUSH normalkey a")
    assert err.startswith("-ERR") or "wrong type" in err.lower()

    # cleanup
    send_command("DEL mylist")
    send_command("DEL normalkey")


def test_4_llen_nonexistent_and_existing():
    send_command("RPUSH mylist a b c")
    res = send_command("LLEN mylist")
    assert res.startswith(":3")

    res = send_command("LLEN noexist")
    assert res.startswith(":0")

    # cleanup
    send_command("DEL mylist")


def test_5_lpop_and_boundaries():
    send_command("RPUSH mylist x y z")
    send_command("SET normalkey hello")

    res = send_command("LPOP mylist")
    assert "$1\r\nx" in res

    res = send_command("LPOP mylist 2")
    assert res.startswith("*2")

    res = send_command("LPOP mylist")
    assert res.strip() == "$-1", f"Expected nil, got {res}"

    err = send_command("LPOP normalkey")
    assert err.lower().startswith("-err")

    # cleanup
    send_command("DEL mylist")
    send_command("DEL normalkey")


# def test_6_blpop_blocking_behavior():
#     helper = subprocess.Popen(
#         ["python", "-c",
#          "import time,socket;"
#          "time.sleep(1);"
#          "s=socket.create_connection(('127.0.0.1',6379));"
#          "p='*3\\r\\n$5\\r\\nRPUSH\\r\\n$8\\r\\nwaitlist\\r\\n$2\\r\\nv1\\r\\n';"
#          "s.sendall(p.encode());"
#          "s.close()"]
#     )

#     res = send_command("BLPOP waitlist 0")
#     helper.wait()
#     assert "*2" in res and "waitlist" in res and "v1" in res

#     start = time.time()
#     res = send_command("BLPOP emptylist 1")
#     elapsed = time.time() - start
#     assert elapsed >= 1
#     assert res.strip() in ("*-1", "$-1"), f"Expected null, got {res}"

#     send_command("DEL waitlist")
#     send_command("DEL emptylist")
