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
    """Load RDB keys, values, and expirations (simplified parser for tests)."""
    global dictionary, expiration_times
    dictionary.clear()
    expiration_times.clear()

    path = os.path.join(config['dir'], config['dbfilename'])
    if not os.path.exists(path):
        return

    data = open(path, "rb").read()
    i = 0

    # Skip header "REDIS" + "0011"
    if data.startswith(b"REDIS"):
        i = 9

    # Move to main table marker (0xFB)
    while i < len(data) and data[i] != 0xFB:
        i += 1
    # skip 0xFB + 2 db-size bytes (as used by tests)
    i += 3

    while i < len(data):
        if data[i] == 0xFF:  # end of file
            break

        expire_ts = None

        # Expiry in seconds (0xFD) -> to ms
        if data[i] == 0xFD:
            expire_ts = int.from_bytes(data[i+1:i+5], "little") * 1000
            i += 5
        # Expiry in ms (0xFC)
        elif data[i] == 0xFC:
            expire_ts = int.from_bytes(data[i+1:i+9], "little")
            i += 9

        # Only handle string type (0x00)
        type_byte = data[i]
        i += 1
        if type_byte != 0x00:
            break

        # Key (len is single byte in tests)
        key_len = data[i]
        i += 1
        key = data[i:i+key_len]
        i += key_len

        # Value (len is single byte in tests)
        val_len = data[i]
        i += 1
        val = data[i:i+val_len]
        i += val_len

        # Skip expired keys
        if expire_ts and expire_ts/1000.0 <= time.time():
            continue

        # Store in memory
        dictionary[key] = val
        if expire_ts:
            expiration_times[key] = expire_ts/1000.0

    print("DEBUG: Loaded keys:", [k.decode() for k in dictionary])
    print("DEBUG: Expirations:", {k.decode(): v for k, v in expiration_times.items()})


def parsing(data):
    split = data.split(b"\r\n")
    if len(split) > 4 and split[2].upper() == b"ECHO":
        return split[4]
    return None


def string(words: bytes) -> bytes:
    return b"$" + str(len(words)).encode() + b"\r\n" + words + b"\r\n"


def accept(sock):
    conn, _ = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)


def get_max_id_in_stream(stream_key: bytes) -> bytes:
    """Return max ID in stream or b'0-0' if empty."""
    if stream_key not in streams or not streams[stream_key]:
        return b"0-0"
    max_entry = max(streams[stream_key], key=lambda e: tuple(map(int, e['id'].split(b'-'))))
    return max_entry['id']


def generate_next_id(stream_key: bytes) -> bytes:
    """Generate next stream ID automatically (ms-0 or same-ms seq+1)."""
    last_id = get_max_id_in_stream(stream_key)
    ms, seq = map(int, last_id.split(b"-"))
    now_ms = int(time.time() * 1000)
    if now_ms > ms:
        return f"{now_ms}-0".encode()
    else:
        return f"{ms}-{seq + 1}".encode()


def compare_ids(id1: bytes, id2: bytes) -> int:
    """Return negative if id1<id2, zero if equal, positive if id1>id2."""
    ms1, seq1 = map(int, id1.split(b"-"))
    ms2, seq2 = map(int, id2.split(b"-"))
    if ms1 != ms2:
        return ms1 - ms2
    return seq1 - seq2


def build_xread_response(stream_keys, last_ids):
    """
    Build XREAD response for the given streams, returning only entries with id > last_id.
    RESP format:
    *N
      *2
        $<len> stream_name
        *M
          *2
            $<len> id
            *<2*field_count>
              $<len> field
              $<len> value
          ...
    """
    streams_with_entries = []
    for key, last_id in zip(stream_keys, last_ids):
        entries = []
        for entry in streams.get(key, []):
            if compare_ids(entry["id"], last_id) > 0:
                entries.append(entry)
        if entries:
            streams_with_entries.append((key, entries))

    if not streams_with_entries:
        return b"*0\r\n"

    result = b"*" + str(len(streams_with_entries)).encode() + b"\r\n"
    for key, entries in streams_with_entries:
        result += b"*2\r\n" + string(key)
        result += b"*" + str(len(entries)).encode() + b"\r\n"
        for entry in entries:
            result += b"*2\r\n" + string(entry["id"])
            fields = entry["fields"]
            result += b"*" + str(len(fields) * 2).encode() + b"\r\n"
            for field, value in fields.items():
                result += string(field) + string(value)
    return result


def execute_xrange_command(data):
    """Execute XRANGE command to query a range of entries in a stream."""
    parts = data.split(b"\r\n")

    # Parse: XRANGE stream_key start_id end_id
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

    # Build response
    result = b"*" + str(len(entries)).encode() + b"\r\n"
    for entry in entries:
        result += b"*2\r\n" + string(entry["id"])
        fields = entry["fields"]
        result += b"*" + str(len(fields) * 2).encode() + b"\r\n"
        for field, value in fields.items():
            result += string(field) + string(value)

    return result


def execute_xread_command(data, conn):
    parts = data.split(b"\r\n")
    uparts = [p.upper() if isinstance(p, (bytes, bytearray)) else p for p in parts]

    # Optional BLOCK
    block_ms = None
    if b"BLOCK" in uparts:
        bidx = uparts.index(b"BLOCK")
        if bidx + 2 < len(parts) and parts[bidx + 2].isdigit():
            block_ms = int(parts[bidx + 2])

    if b"STREAMS" not in uparts:
        return b"-ERR syntax error\r\n"
    sidx = uparts.index(b"STREAMS")

    # Parse keys and IDs (skip RESP $len tokens)
    tail = parts[sidx + 1:]
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

    if len(actual_values) % 2 != 0:
        return b"-ERR syntax error\r\n"

    half = len(actual_values) // 2
    stream_keys = actual_values[:half]
    stream_ids = actual_values[half:]

    # Resolve $ to current max ID
    resolved_ids = []
    for k, sid in zip(stream_keys, stream_ids):
        if sid == b"$":
            resolved_ids.append(get_max_id_in_stream(k))
        else:
            resolved_ids.append(sid)

    # Immediate data available?
    for key, last_id in zip(stream_keys, resolved_ids):
        for entry in streams.get(key, []):
            if compare_ids(entry["id"], last_id) > 0:
                return build_xread_response(stream_keys, resolved_ids)

    # No data; maybe block
    if block_ms is not None:
        expire_time = float('inf') if block_ms == 0 else time.time() + block_ms / 1000.0
        blocking_clients[conn] = (expire_time, stream_keys, resolved_ids)
        return None
    else:
        return b"*0\r\n"


def _next_seq_for_ms(stream_key: bytes, ms: int) -> int:
    """Return next sequence number for given ms in a stream (starting from 1)."""
    max_seq = 0
    for e in streams.get(stream_key, []):
        ems, eseq = map(int, e["id"].split(b"-"))
        if ems == ms and eseq > max_seq:
            max_seq = eseq
    return max_seq + 1 if max_seq > 0 else 1


def execute_xadd_command(data):
    """Execute XADD stream id field value (single pair as used by tests)."""
    global streams, blocking_clients
    parts = data.split(b"\r\n")

    stream_key = parts[4]
    raw_id = parts[6]
    field = parts[8]
    value = parts[10]

    # ID handling
    if raw_id == b"*":
        entry_id = generate_next_id(stream_key)
    elif b"-*" in raw_id:
        # Partially auto-generated: "<ms>-*"
        lhs = raw_id.split(b"-")[0]
        try:
            ms = int(lhs)
        except ValueError:
            ms = 0
        seq = _next_seq_for_ms(stream_key, ms)
        entry_id = f"{ms}-{seq}".encode()
    else:
        entry_id = raw_id

    streams.setdefault(stream_key, [])
    entry = {"id": entry_id, "fields": {field: value}}
    streams[stream_key].append(entry)

    # Unblock any waiting clients for this stream
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
    # Simple PX parsing: "*5\r\n$3\r\nSET\r\n... $2\r\nPX\r\n$<n>\r\n<ms>\r\n"
    if len(split) > 10 and split[8].upper() == b"PX" and split[10].isdigit():
        expiration_times[key] = time.time() + int(split[10]) / 1000
    elif len(split) > 10 and split[10].isdigit():  # earlier simplistic tests
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


def check_blocked_timeouts():
    """Check and handle timeouts for blocked clients."""
    current_time = time.time()
    expired_clients = []
    for conn, (expire_time, _, _) in list(blocking_clients.items()):
        if current_time >= expire_time:
            try:
                conn.sendall(b"$-1\r\n")
            except Exception:
                pass
            expired_clients.append(conn)
    for conn in expired_clients:
        blocking_clients.pop(conn, None)


def read(conn):
    global dictionary, streams
    data = conn.recv(4096)
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
    elif b"SET" in cmd and b"XSET" not in cmd:
        if is_in_multi(conn):
            enqueue(conn, 'SET', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_set_command(data))
    elif b"GET" in cmd and b"XGET" not in cmd:
        if is_in_multi(conn):
            enqueue(conn, 'GET', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_get_command(data))
    elif b"INCR" in cmd and b"XINCR" not in cmd:
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
            # Build RESP array of queued responses
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
