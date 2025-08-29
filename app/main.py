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

# ---------------------------
# RDB LOADING (strings + expirations)
# ---------------------------

def load_rdb():
    """Load RDB keys, values, and expirations (strings only)."""
    global dictionary, expiration_times
    dictionary.clear()
    expiration_times.clear()

    path = os.path.join(config['dir'], config['dbfilename'])
    if not os.path.exists(path):
        return

    data = open(path, "rb").read()
    i = 0

    # Skip header like: "REDIS0009"
    if data.startswith(b"REDIS"):
        i = 9

    # Move to main DB selector (0xFE) or main table (0xFB)
    while i < len(data) and data[i] not in (0xFB, 0xFE):
        i += 1

    # If 0xFE (SELECTDB), skip db number (1 byte here for the simple files used in tests)
    if i < len(data) and data[i] == 0xFE:
        i += 2  # 0xFE + db number byte

    # Expect hash table start 0xFB; skip header count bytes (2 for these simple dumps)
    while i < len(data) and data[i] != 0xFB:
        i += 1
    if i < len(data) and data[i] == 0xFB:
        i += 3  # 0xFB + 2 size bytes

    # Entries loop
    while i < len(data):
        if data[i] == 0xFF:  # end of file
            break

        expire_ts_ms = None

        # Expire time in seconds (0xFD) -> 4 bytes little-endian seconds, convert to ms
        if i < len(data) and data[i] == 0xFD:
            if i + 5 > len(data):
                break
            expire_ts_ms = int.from_bytes(data[i+1:i+5], "little") * 1000
            i += 5
        # Expire time in ms (0xFC) -> 8 bytes little-endian ms
        elif i < len(data) and data[i] == 0xFC:
            if i + 9 > len(data):
                break
            expire_ts_ms = int.from_bytes(data[i+1:i+9], "little")
            i += 9

        # Type byte (only simple string = 0x00 supported here)
        if i >= len(data):
            break
        type_byte = data[i]
        i += 1
        if type_byte != 0x00:
            # Skip unsupported types in this stage
            # (In a fuller parser we'd decode by type; for the stage tests, strings suffice.)
            continue

        # Decode simple 1-byte length-encoded key
        if i >= len(data):
            break
        key_len = data[i]
        i += 1
        if i + key_len > len(data):
            break
        key = data[i:i+key_len]
        i += key_len

        # Decode simple 1-byte length-encoded value
        if i >= len(data):
            break
        val_len = data[i]
        i += 1
        if i + val_len > len(data):
            break
        val = data[i:i+val_len]
        i += val_len

        # Skip already-expired keys
        if expire_ts_ms is not None and (expire_ts_ms / 1000.0) <= time.time():
            continue

        dictionary[key] = val
        if expire_ts_ms is not None:
            expiration_times[key] = expire_ts_ms / 1000.0

# ---------------------------
# RESP helpers
# ---------------------------

def parsing(data):
    split = data.split(b"\r\n")
    if len(split) > 4 and split[2] == b"ECHO":
        return split[4]
    return None

def string(words):
    return b"$" + str(len(words)).encode() + b"\r\n" + words + b"\r\n"

def null_bulk():
    return b"$-1\r\n"

# ---------------------------
# Networking
# ---------------------------

def accept(sock):
    conn, _ = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

# ---------------------------
# Streams helpers
# ---------------------------

def get_max_id_in_stream(stream_key):
    if stream_key not in streams or not streams[stream_key]:
        return b"0-0"
    max_entry = max(streams[stream_key], key=lambda e: tuple(map(int, e['id'].split(b'-'))))
    return max_entry['id']

def generate_next_id(stream_key, raw_id=None):
    """
    Generate next stream ID.
    - raw_id == b"*" -> current_time_ms + next seq (0 if first in ms bucket)
    - raw_id endswith b"-*" -> use provided ms, next seq
    """
    now_ms = int(time.time() * 1000)
    streams.setdefault(stream_key, [])

    def next_seq_for_ms(ms):
        existing = [e for e in streams[stream_key] if int(e["id"].split(b"-")[0]) == ms]
        if existing:
            return max(int(e["id"].split(b"-")[1]) for e in existing) + 1
        return 0

    if raw_id is None or raw_id == b"*":
        ms = now_ms
        seq = next_seq_for_ms(ms)
        return f"{ms}-{seq}".encode()

    if raw_id.endswith(b"-*"):
        ms = int(raw_id.split(b"-")[0])
        seq = next_seq_for_ms(ms)
        return f"{ms}-{seq}".encode()

    # explicit ID passed; use as-is (caller should validate monotonicity if needed)
    return raw_id

def compare_ids(id1, id2):
    ms1, seq1 = map(int, id1.split(b"-"))
    ms2, seq2 = map(int, id2.split(b"-"))
    if ms1 != ms2:
        return ms1 - ms2
    return seq1 - seq2

def build_xread_response(stream_keys, resolved_ids):
    result = b"*" + str(len(stream_keys)).encode() + b"\r\n"
    for stream_key, last_id in zip(stream_keys, resolved_ids):
        new_entries = [e for e in streams.get(stream_key, []) if compare_ids(e["id"], last_id) > 0]
        result += b"*2\r\n" + string(stream_key)
        result += b"*" + str(len(new_entries)).encode() + b"\r\n"
        for entry in new_entries:
            result += b"*2\r\n" + string(entry["id"])
            fields = entry["fields"]
            result += b"*" + str(len(fields) * 2).encode() + b"\r\n"
            for f, v in fields.items():
                result += string(f) + string(v)
    return result

# ---------------------------
# Command executors
# ---------------------------

def execute_xrange_command(data):
    parts = data.split(b"\r\n")
    stream_key = parts[4]
    start_id = parts[6]
    end_id = parts[8]
    if stream_key not in streams:
        return b"*0\r\n"

    entries = []
    for entry in streams[stream_key]:
        entry_id = entry["id"]
        if start_id != b"-" and compare_ids(entry_id, start_id) < 0:
            continue
        if end_id != b"+" and compare_ids(entry_id, end_id) > 0:
            continue
        entries.append(entry)

    result = b"*" + str(len(entries)).encode() + b"\r\n"
    for entry in entries:
        result += b"*2\r\n" + string(entry["id"])
        fields = entry["fields"]
        result += b"*" + str(len(fields) * 2).encode() + b"\r\n"
        for f, v in fields.items():
            result += string(f) + string(v)
    return result

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
    tail = parts[sidx + 1:]

    # Pull out actual data (skip RESP headers like $N, etc.)
    actual_values = []
    i = 0
    while i < len(tail):
        if tail[i].startswith(b'$') and i + 1 < len(tail):
            actual_values.append(tail[i + 1])
            i += 2
        elif tail[i] != b"":
            actual_values.append(tail[i])
            i += 1
        else:
            i += 1

    half = len(actual_values) // 2
    stream_keys = actual_values[:half]
    stream_ids = actual_values[half:]

    resolved_ids = []
    for k, sid in zip(stream_keys, stream_ids):
        if sid == b"$":
            resolved_ids.append(get_max_id_in_stream(k))
        else:
            resolved_ids.append(sid)

    has_new_entries = False
    for key, last_id in zip(stream_keys, resolved_ids):
        for entry in streams.get(key, []):
            if compare_ids(entry["id"], last_id) > 0:
                has_new_entries = True
                break
        if has_new_entries:
            break

    if has_new_entries:
        return build_xread_response(stream_keys, resolved_ids)

    if block_ms is not None:
        expire_time = float('inf') if block_ms == 0 else time.time() + block_ms / 1000.0
        blocking_clients[conn] = (expire_time, stream_keys, resolved_ids)
        return None
    else:
        return b"*0\r\n"

def execute_xadd_command(data):
    global streams, blocking_clients
    parts = data.split(b"\r\n")
    stream_key = parts[4]
    raw_id = parts[6]
    field = parts[8]
    value = parts[10]

    entry_id = generate_next_id(stream_key, raw_id)

    streams.setdefault(stream_key, [])
    entry = {"id": entry_id, "fields": {field: value}}
    streams[stream_key].append(entry)

    # Unblock clients waiting on this stream
    to_unblock = []
    for c, (expire_time, keys, ids) in list(blocking_clients.items()):
        if stream_key in keys:
            idx = keys.index(stream_key)
            last_id = ids[idx]
            if compare_ids(entry_id, last_id) > 0:
                resp = build_xread_response([stream_key], [last_id])
                try:
                    c.sendall(resp)
                except Exception:
                    pass
                to_unblock.append(c)

    for c in to_unblock:
        blocking_clients.pop(c, None)

    return string(entry_id)

def is_in_multi(conn):
    return conn in transactions and transactions[conn]["in_multi"]

def enqueue(conn, cmd, data):
    transactions.setdefault(conn, {"in_multi": True, "queue": []})
    transactions[conn]["queue"].append((cmd, data))

def execute_keys_command(_):
    # Purge expired keys first to avoid showing dead ones
    now = time.time()
    expired = [k for k, t in expiration_times.items() if now >= t]
    for k in expired:
        dictionary.pop(k, None)
        expiration_times.pop(k, None)

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

    # Handle "PX <ms>" (very basic position-dependent parsing for this stage)
    # Example RESP: *6 \r\n $3 \r\n SET \r\n $3 \r\n key \r\n $5 \r\n value \r\n $2 \r\n PX \r\n $3 \r\n 100 \r\n
    # The PX value tends to land at split[10] here.
    if len(split) > 10 and split[8].upper() == b"PX" and split[10].isdigit():
        expiration_times[key] = time.time() + int(split[10]) / 1000.0
    else:
        # If SET without PX on an existing key, clear any previous expiration
        if key in expiration_times:
            del expiration_times[key]
    return b"+OK\r\n"

def execute_get_command(data):
    global dictionary, expiration_times
    split = data.split(b"\r\n")
    key = split[4]

    # Key missing
    if key not in dictionary:
        return null_bulk()

    # Check expiration
    if key in expiration_times and time.time() >= expiration_times[key]:
        # purge and return null
        del dictionary[key]
        del expiration_times[key]
        return null_bulk()

    return string(dictionary[key])

def execute_incr_command(data):
    global dictionary, expiration_times
    split = data.split(b"\r\n")
    key = split[4]

    # Expired?
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

    # Purge if expired
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

def check_blocked_timeouts():
    current_time = time.time()
    expired_clients = []
    for conn, (expire_time, stream_keys, resolved_ids) in list(blocking_clients.items()):
        if current_time >= expire_time:
            # For XREAD timeouts, Redis returns a null multi-bulk reply (here we return *-1-like),
            # but to keep stage focus minimal, respond with a null bulk to unblock.
            try:
                conn.sendall(null_bulk())
            except Exception:
                pass
            expired_clients.append(conn)
    for conn in expired_clients:
        blocking_clients.pop(conn, None)

# ---------------------------
# IO loop
# ---------------------------

def read(conn):
    global dictionary, streams
    try:
        data = conn.recv(1024)
    except ConnectionResetError:
        data = b""
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
    elif b"XREAD" in cmd:
        resp = execute_xread_command(data, conn)
        if resp:
            conn.sendall(resp)
    elif b"XADD" in cmd:
        conn.sendall(execute_xadd_command(data))
    elif b"XRANGE" in cmd:
        conn.sendall(execute_xrange_command(data))
    elif b"MULTI" in cmd:
        transactions[conn] = {"in_multi": True, "queue": []}
        conn.sendall(b"+OK\r\n")
    elif b"DISCARD" in cmd:
        if is_in_multi(conn):
            transactions.pop(conn, None)
            conn.sendall(b"+OK\r\n")
        else:
            conn.sendall(b"-ERR DISCARD without MULTI\r\n")
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
        check_blocked_timeouts()

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
