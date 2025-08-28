import socket
import selectors
import time
import sys
import os

sel = selectors.DefaultSelector()
dictionary = {}
expiration_times = {}
streams = {}
blocking_clients = {}
transactions = {}  # per-connection MULTI state

config = {
    'dir': '/tmp',
    'dbfilename': 'dump.rdb'
}
def load_rdb():
    """Properly parse RDB files: handles multiple keys, values, expirations."""
    global dictionary, expiration_times
    dictionary.clear()
    expiration_times.clear()

    rdb_path = os.path.join(config['dir'], config['dbfilename'])
    if not os.path.exists(rdb_path):
        return

    with open(rdb_path, "rb") as f:
        data = f.read()

    i = 0
    if data[:5] == b"REDIS":
        i = 9

    # Skip until FB marker (database section start)
    while i < len(data) and data[i] != 0xFB:
        if data[i] == 0xFA:
            name_len = data[i + 1]
            i += 2 + name_len
            val_len = data[i]
            i += 1 + val_len
            continue
        if data[i] == 0xFE:
            i += 2
            continue
        i += 1

    # Skip FB marker + 2 size bytes
    i += 3

    while i < len(data):
        if data[i] == 0xFF:
            break

        expire_ts = None

        # Handle expiry markers
        if data[i] == 0xFD:
            expire_ts = int.from_bytes(data[i + 1:i + 5], "little") * 1000
            i += 5
        elif data[i] == 0xFC:
            expire_ts = int.from_bytes(data[i + 1:i + 9], "little")
            i += 9

        # Read TYPE byte (always present after expiry marker if any)
        type_byte = data[i]
        i += 1

        # We only support string values (type 0x00)
        if type_byte != 0x00:
            print(f"Unsupported type: {type_byte}")
            return

        # Read key
        key_len = data[i]
        i += 1
        key = data[i:i + key_len]
        i += key_len

        # Read value
        val_len = data[i]
        i += 1
        value = data[i:i + val_len]
        i += val_len

        # Skip expired keys immediately
        if expire_ts is not None and expire_ts / 1000.0 <= time.time():
            continue

        dictionary[key] = value
        if expire_ts is not None:
            expiration_times[key] = expire_ts / 1000.0

    print("DEBUG: Loaded keys =", [k.decode() for k in dictionary])
    print("DEBUG: Expirations =", {k.decode(): expiration_times[k] for k in expiration_times})


def parsing(data):
    split = data.split(b"\r\n")
    if len(split) > 4 and split[2] == b"ECHO":
        return split[4]
    return None


def string(words):
    return b"$" + str(len(words)).encode() + b"\r\n" + words + b"\r\n"


def accept(sock):
    conn, _ = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)


def get_max_id_in_stream(stream_key):
    if stream_key not in streams or not streams[stream_key]:
        return b"0-0"
    max_entry = max(streams[stream_key], key=lambda entry: tuple(map(int, entry['id'].split(b'-'))))
    return max_entry['id']


def is_in_multi(conn):
    return conn in transactions and transactions[conn]["in_multi"]


def enqueue(conn, cmd, data):
    transactions.setdefault(conn, {"in_multi": True, "queue": []})
    transactions[conn]["queue"].append((cmd, data))


def execute_keys_command(_):
    keys = list(dictionary.keys())
    result = b"*" + str(len(keys)).encode() + b"\r\n"
    for key in keys:
        result += string(key)
    return result


def execute_set_command(data):
    global dictionary, expiration_times
    split = data.split(b"\r\n")
    key = split[4]
    value = split[6]
    dictionary[key] = value
    if len(split) > 10 and split[10].isdigit():
        expiration_times[key] = time.time() + int(split[10]) / 1000
    elif key in expiration_times:
        del expiration_times[key]
    return b"+OK\r\n"


def execute_get_command(data):
    global dictionary, expiration_times
    split = data.split(b"\r\n")
    key = split[4]
    # Key missing
    if key not in dictionary:
        return b"$-1\r\n"
    # Key expired
    if key in expiration_times and time.time() >= expiration_times[key]:
        del dictionary[key]
        del expiration_times[key]
        return b"$-1\r\n"
    return string(dictionary[key])


def execute_incr_command(data):
    global dictionary, expiration_times
    split = data.split(b"\r\n")
    key = split[4]
    if key in expiration_times and time.time() >= expiration_times[key]:
        del dictionary[key]
        del expiration_times[key]
    if key in dictionary:
        try:
            current_value = int(dictionary[key])
            new_value = current_value + 1
            dictionary[key] = str(new_value).encode()
            return b":" + str(new_value).encode() + b"\r\n"
        except ValueError:
            return b"-ERR value is not an integer or out of range\r\n"
    dictionary[key] = b"1"
    return b":1\r\n"


def execute_type_command(data):
    split = data.split(b"\r\n")
    key = split[4]
    if key in expiration_times and time.time() >= expiration_times[key]:
        del dictionary[key]
        del expiration_times[key]
    if key in streams:
        return b'+stream\r\n'
    elif key in dictionary:
        return b'+string\r\n'
    return b"+none\r\n"


def execute_config_get_command(data):
    split = data.split(b"\r\n")
    param = split[6]
    if param == b'dir':
        value = config['dir'].encode()
    elif param == b'dbfilename':
        value = config['dbfilename'].encode()
    else:
        return b"*0\r\n"
    result = b"*2\r\n" + string(param) + string(value)
    return result


def read(conn):
    global dictionary, streams
    data = conn.recv(1024)
    if not data:
        sel.unregister(conn)
        conn.close()
        transactions.pop(conn, None)
        blocking_clients.pop(conn, None)
        return

    cmd = data.upper()

    if b"PING" in cmd:
        conn.sendall(b"+PONG\r\n")
    elif b"KEYS" in cmd:
        conn.sendall(execute_keys_command(data))
    elif b"CONFIG" in cmd and b"GET" in cmd:
        conn.sendall(execute_config_get_command(data))
    elif b"SET" in cmd:
        if is_in_multi(conn):
            enqueue(conn, 'SET', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_set_command(data))
    elif b"GET" in cmd:
        if is_in_multi(conn):
            enqueue(conn, 'GET', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_get_command(data))
    elif b"INCR" in cmd:
        if is_in_multi(conn):
            enqueue(conn, 'INCR', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_incr_command(data))
    elif b"TYPE" in cmd:
        if is_in_multi(conn):
            enqueue(conn, 'TYPE', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_type_command(data))
    elif b"MULTI" in cmd:
        transactions[conn] = {"in_multi": True, "queue": []}
        conn.sendall(b"+OK\r\n")
    elif b"DISCARD" in cmd:
        transactions.pop(conn, None)
        conn.sendall(b"+OK\r\n")
    elif b"EXEC" in cmd:
        if not is_in_multi(conn):
            conn.sendall(b"-ERR EXEC without MULTI\r\n")
        else:
            responses = []
            for command_type, command_data in transactions[conn]["queue"]:
                if command_type == 'SET':
                    responses.append(execute_set_command(command_data))
                elif command_type == 'GET':
                    responses.append(execute_get_command(command_data))
                elif command_type == 'INCR':
                    responses.append(execute_incr_command(command_data))
                elif command_type == 'TYPE':
                    responses.append(execute_type_command(command_data))
            result = b"*" + str(len(responses)).encode() + b"\r\n" + b"".join(responses)
            conn.sendall(result)
            transactions.pop(conn, None)
    else:
        temp = parsing(data)
        if temp:
            conn.sendall(string(temp))
        else:
            conn.sendall(b"-ERR unknown command\r\n")


def main(port=6379):
    load_rdb()
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    server_socket.setblocking(False)
    sel.register(server_socket, selectors.EVENT_READ, accept)
    while True:
        events = sel.select(timeout=0.1)
        for key, _ in events:
            callback = key.data
            callback(key.fileobj)


if __name__ == "__main__":
    port = 6379
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        port = int(sys.argv[idx + 1])
    if "--dir" in sys.argv:
        idx = sys.argv.index("--dir")
        config['dir'] = sys.argv[idx + 1]
    if "--dbfilename" in sys.argv:
        idx = sys.argv.index("--dbfilename")
        config['dbfilename'] = sys.argv[idx + 1]
    main(port)
