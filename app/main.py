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

# ------------------ RDB LOADING ------------------

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
        i = 9  # skip "REDIS0011" header-ish

    # skip to DB selector (0xFE) and RDB AUX fields until first db
    while i < len(data) and data[i] != 0xFB:
        i += 1
    i += 3  # skip 0xFB, db number, and 0x00 hash table size (in our crafted rdbs)

    while i < len(data):
        if data[i] == 0xFF:  # EOF
            break

        expire_ts = None
        if data[i] == 0xFD:               # expire seconds
            expire_ts = int.from_bytes(data[i+1:i+5], "little") * 1000
            i += 5
        elif data[i] == 0xFC:             # expire ms
            expire_ts = int.from_bytes(data[i+1:i+9], "little")
            i += 9

        type_byte = data[i]
        i += 1
        if type_byte != 0x00:  # only simple string values in tests
            break

        key_len = data[i]; i += 1
        key = data[i:i+key_len]; i += key_len

        val_len = data[i]; i += 1
        val = data[i:i+val_len]; i += val_len

        # skip expired
        if expire_ts and expire_ts/1000.0 <= time.time():
            continue

        dictionary[key] = val
        if expire_ts:
            expiration_times[key] = expire_ts/1000.0

    print("DEBUG: Loaded keys:", [k.decode() for k in dictionary])
    print("DEBUG: Expirations:", {k.decode(): v for k,v in expiration_times.items()})


# ------------------ RESP HELPERS ------------------

def parsing(data):
    split = data.split(b"\r\n")
    if len(split) > 4 and split[2] == b"ECHO":
        return split[4]
    return None

def string(words: bytes) -> bytes:
    return b"$" + str(len(words)).encode() + b"\r\n" + words + b"\r\n"

def resp_values(data: bytes):
    """
    Extract only value tokens from a RESP2-encoded command:
    - Ignore array headers (*N)
    - For $len, take the next line as the value
    - For :integer, take the number as value (stringified bytes)
    - (Safety) if a bare word appears (rare), keep it
    Result is an ordered list of tokens (bytes).
    """
    lines = data.split(b"\r\n")
    vals = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line == b"":
            i += 1
            continue
        if line.startswith(b"*"):
            i += 1
            continue
        if line.startswith(b"$"):
            # next line should be the bulk string value
            if i + 1 < len(lines):
                vals.append(lines[i+1])
                i += 2
            else:
                i += 1
            continue
        if line.startswith(b":"):
            vals.append(line[1:])
            i += 1
            continue
        # Fallback: keep the line (e.g., inline commands)
        vals.append(line)
        i += 1
    return vals


# ------------------ BASIC COMMANDS ------------------

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
    # Optional PX
    if len(split) > 10 and split[8].upper() == b"PX" and split[10].isdigit():
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

    # parse field-value pairs (each is 4 steps: $len, field, $len, value)
    fields = {}
    i = 8
    while i + 2 < len(parts) and parts[i]:
        field = parts[i]          # actual field name
        value = parts[i+2]        # actual value
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
    tokens = resp_values(data)
    utokens = [t.upper() for t in tokens]

    # Expect: XREAD [BLOCK ms] STREAMS k1 k2 ... id1 id2 ...
    if not tokens or utokens[0] != b"XREAD":
        return b"-ERR syntax error\r\n"

    block_ms = None
    i = 1
    while i < len(tokens):
        t = utokens[i]
        if t == b"BLOCK" and i + 1 < len(tokens):
            try:
                block_ms = int(tokens[i+1])
            except ValueError:
                return b"-ERR value is not an integer or out of range\r\n"
            i += 2
        elif t == b"STREAMS":
            i += 1
            break
        else:
            i += 1

    if i >= len(tokens):
        return b"-ERR syntax error\r\n"

    # Remaining: stream names then IDs (same count)
    rest = tokens[i:]
    if len(rest) < 2:
        return b"-ERR syntax error\r\n"

    # split names & ids
    half = len(rest) // 2
    stream_keys = rest[:half]
    stream_ids = rest[half:]

    # Resolve '$' to the stream's current last ID
    resolved_ids = []
    for stream, sid in zip(stream_keys, stream_ids):
        if sid == b"$":
            last_list = streams.get(stream, [])
            since = last_list[-1]['id'] if last_list else b"0-0"
            resolved_ids.append(since)
        else:
            resolved_ids.append(sid)

    # Try immediate read
    resp = build_xread_response(stream_keys, resolved_ids)
    if resp:
        return resp

    # Otherwise, maybe block
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
                # fields array: ["field","value",...]
                fv = []
                for k, v in e['fields'].items():
                    fv.append(string(k))
                    fv.append(string(v))
                entry = b"*" + b"2\r\n" + string(e['id']) + b"*" + str(len(fv)).encode() + b"\r\n" + b"".join(fv)
                entries.append(entry)
        if entries:
            arr = b"*" + str(len(entries) + 1).encode() + b"\r\n" + string(stream) + b"".join(entries)
            outer.append(arr)

    if not outer:
        return None
    return b"*" + str(len(outer)).encode() + b"\r\n" + b"".join(outer)

def compare_ids(a: bytes, b: bytes) -> int:
    ats, asq = map(int, a.split(b"-"))
    bts, bsq = map(int, b.split(b"-"))
    if ats != bts:
        return 1 if ats > bts else -1
    if asq != bsq:
        return 1 if asq > bsq else -1
    return 0

def wake_blocked_clients(stream):
    to_wake = []
    for conn, (exp, keys, ids) in list(blocking_clients.items()):
        if stream in keys:
            resp = build_xread_response(keys, ids)
            if resp:
                try:
                    conn.sendall(resp)
                except Exception:
                    pass
                to_wake.append(conn)
    for c in to_wake:
        blocking_clients.pop(c, None)


# ------------------ SERVER LOOP ------------------

def read(conn):
    global dictionary, streams
    try:
        data = conn.recv(1024)
    except ConnectionResetError:
        data = b""
    if not data:
        try:
            sel.unregister(conn)
        except Exception:
            pass
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
    elif b"GET\r\n" in cmd or cmd.startswith(b"*2\r\n$3\r\nGET"):
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

        # Unblock timed-out XREADs
        now = time.time()
        for c, (exp, _, _) in list(blocking_clients.items()):
            if exp != float("inf") and now > exp:
                try:
                    c.sendall(b"*0\r\n")
                except Exception:
                    pass
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
