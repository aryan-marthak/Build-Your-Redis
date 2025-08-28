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
transactions = {}

config = {
    'dir': '/tmp',
    'dbfilename': 'dump.rdb'
}

def load_rdb():
    global dictionary, expiration_times
    dictionary.clear()
    expiration_times.clear()
    path = os.path.join(config['dir'], config['dbfilename'])
    if not os.path.exists(path):
        return

    data = open(path, "rb").read()
    i = 0
    if data.startswith(b"REDIS"):
        i = 9

    while i < len(data) and data[i] != 0xFB:
        i += 1
    i += 3

    while i < len(data):
        if data[i] == 0xFF:
            break

        expire_ts = None
        if data[i] == 0xFD:
            expire_ts = int.from_bytes(data[i+1:i+5], "little") * 1000
            i += 5
        elif data[i] == 0xFC:
            expire_ts = int.from_bytes(data[i+1:i+9], "little")
            i += 9

        type_byte = data[i]
        i += 1
        if type_byte != 0x00:
            break

        key_len = data[i]; i += 1
        key = data[i:i+key_len]; i += key_len

        val_len = data[i]; i += 1
        val = data[i:i+val_len]; i += val_len

        if expire_ts and expire_ts/1000.0 <= time.time():
            continue

        dictionary[key] = val
        if expire_ts:
            expiration_times[key] = expire_ts/1000.0

    print("DEBUG: Loaded keys:", [k.decode() for k in dictionary])
    print("DEBUG: Expirations:", {k.decode(): v for k,v in expiration_times.items()})


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
    if key not in dictionary:
        return b"$-1\r\n"
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

# ------------------ STREAMS ------------------

def execute_xadd_command(data):
    global streams
    parts = data.split(b"\r\n")
    stream = parts[4]
    entry_id = parts[6]
    fields = {}
    i = 8
    while i < len(parts) and parts[i]:
        field = parts[i]
        value = parts[i+2]
        fields[field] = value
        i += 4

    if entry_id == b"*":
        ms = int(time.time() * 1000)
        seq = len(streams.get(stream, []))
        entry_id = f"{ms}-{seq}".encode()

    streams.setdefault(stream, [])
    streams[stream].append({"id": entry_id, "fields": fields})

    wake_blocked_clients(stream)

    return string(entry_id)

def execute_xread_command(data, conn):
    parts = data.split(b"\r\n")
    uparts = [p.upper() if isinstance(p, (bytes, bytearray)) else p for p in parts]
    block_ms = None
    if b"BLOCK" in uparts:
        bidx = uparts.index(b"BLOCK")
        if bidx + 2 < len(parts) and parts[bidx + 2].isdigit():
            block_ms = int(parts[bidx + 2])
    if b"STREAMS" not in uparts:
        return b"-ERR syntax error\r\n"
    sidx = uparts.index(b"STREAMS")
    tail = parts[sidx + 2:]
    tail = [t for t in tail if t != b""]
    half = len(tail) // 2
    stream_keys = tail[:half]
    stream_ids = tail[half:]

    resolved_ids = []
    for k, sid in zip(stream_keys, stream_ids):
        resolved_ids.append(sid if sid != b"$" else b"0-0")

    resp = build_xread_response(stream_keys, resolved_ids)
    if resp:
        return resp

    if block_ms is not None:
        expire_time = float('inf') if block_ms == 0 else time.time() + block_ms / 1000.0
        blocking_clients[conn] = (expire_time, stream_keys, resolved_ids)
        return None
    else:
        return b"*0\r\n"

def build_xread_response(stream_keys, resolved_ids):
    outer = []
    for stream, since_id in zip(stream_keys, resolved_ids):
        entries = []
        for e in streams.get(stream, []):
            if compare_ids(e['id'], since_id) > 0:
                fields = e['fields']
                fv = []
                for k,v in fields.items():
                    fv.append(string(k))
                    fv.append(string(v))
                entry = b"*" + b"2\r\n" + string(e['id']) + b"*" + str(len(fv)).encode() + b"\r\n" + b"".join(fv)
                entries.append(entry)
        if entries:
            arr = b"*" + str(len(entries)+1).encode() + b"\r\n" + string(stream) + b"".join(entries)
            outer.append(arr)
    if not outer:
        return None
    return b"*" + str(len(outer)).encode() + b"\r\n" + b"".join(outer)

def compare_ids(a, b):
    ats, asq = map(int, a.split(b"-"))
    bts, bsq = map(int, b.split(b"-"))
    return (ats > bts) - (ats < bts) or (asq > bsq) - (asq < bsq)

def wake_blocked_clients(stream):
    to_wake = []
    for conn, (exp, keys, ids) in list(blocking_clients.items()):
        if stream in keys:
            resp = build_xread_response(keys, ids)
            if resp:
                conn.sendall(resp)
                to_wake.append(conn)
    for c in to_wake:
        blocking_clients.pop(c, None)

# ------------------ SERVER LOOP ------------------

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
    elif b"XADD" in cmd:
        conn.sendall(execute_xadd_command(data))
    elif b"XREAD" in cmd:
        resp = execute_xread_command(data, conn)
        if resp:
            conn.sendall(resp)
    elif b"MULTI" in cmd:
        transactions[conn] = {"in_multi": True, "queue": []}
        conn.sendall(b"+OK\r\n")
    elif b"DISCARD" in cmd:
        if not is_in_multi(conn):
            conn.sendall(b"-ERR DISCARD without MULTI\r\n")
        else:
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
        now = time.time()
        for c, (exp, _, _) in list(blocking_clients.items()):
            if exp != float("inf") and now > exp:
                c.sendall(b"*0\r\n")
                blocking_clients.pop(c, None)
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
