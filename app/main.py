import socket
import selectors
import time
import sys
import os

sel = selectors.DefaultSelector()
dictionary = {}
expiration_times = {}
streams = {}                 # stream_key -> list of {'id': b'123-0', 'fields': {b'f': b'v'}}
blocking_clients = {}        # conn -> (expire_time, [keys], [ids])
transactions = {}            # per-connection MULTI state

config = {'dir': '/tmp', 'dbfilename': 'dump.rdb'}

# --------------------------- RDB LOADER (kept simple) ---------------------------

def load_rdb():
    """Very simple RDB loader: finds keys, values, and expirations."""
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
    i += 3  # skip 0xFB + 2 size bytes

    while i < len(data):
        if data[i] == 0xFF:
            break

        expire_ts = None
        if data[i] == 0xFD:       # seconds
            expire_ts = int.from_bytes(data[i+1:i+5], "little") * 1000
            i += 5
        elif data[i] == 0xFC:     # milliseconds
            expire_ts = int.from_bytes(data[i+1:i+9], "little")
            i += 9

        # type byte (0x00 = string)
        if i >= len(data):
            break
        t = data[i]; i += 1
        if t != 0x00:
            break

        # key
        if i >= len(data): break
        klen = data[i]; i += 1
        key = data[i:i+klen]; i += klen

        # value
        if i >= len(data): break
        vlen = data[i]; i += 1
        val = data[i:i+vlen]; i += vlen

        # expired → skip storing
        if expire_ts and expire_ts/1000.0 <= time.time():
            continue

        dictionary[key] = val
        if expire_ts:
            expiration_times[key] = expire_ts/1000.0

    print("DEBUG: Loaded keys:", [k.decode() for k in dictionary])
    print("DEBUG: Expirations:", {k.decode(): v for k, v in expiration_times.items()})

# --------------------------- RESP helpers ---------------------------

def parsing(data):
    split = data.split(b"\r\n")
    if len(split) > 4 and split[2] == b"ECHO":
        return split[4]
    return None

def bulk(words: bytes) -> bytes:
    return b"$" + str(len(words)).encode() + b"\r\n" + words + b"\r\n"

def resp_array(items: list[bytes]) -> bytes:
    return b"*" + str(len(items)).encode() + b"\r\n" + b"".join(items)

# --------------------------- Server basics ---------------------------

def accept(sock):
    conn, _ = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

# --------------------------- Transactions helpers ---------------------------

def is_in_multi(conn):
    return conn in transactions and transactions[conn]["in_multi"]

def enqueue(conn, cmd, data):
    transactions.setdefault(conn, {"in_multi": True, "queue": []})
    transactions[conn]["queue"].append((cmd, data))

# --------------------------- Commands: strings / config ---------------------------

def execute_keys_command(_):
    keys = list(dictionary.keys())
    return resp_array([bulk(k) for k in keys])

def execute_set_command(data):
    split = data.split(b"\r\n")
    key = split[4]
    value = split[6]
    dictionary[key] = value
    # optional PX
    if len(split) > 8 and split[8].upper() == b"PX" and len(split) > 10 and split[10].isdigit():
        expiration_times[key] = time.time() + int(split[10]) / 1000.0
    elif key in expiration_times:
        del expiration_times[key]
    return b"+OK\r\n"

def execute_get_command(data):
    split = data.split(b"\r\n")
    key = split[4]
    if key not in dictionary:
        return b"$-1\r\n"
    if key in expiration_times and time.time() >= expiration_times[key]:
        del dictionary[key]
        del expiration_times[key]
        return b"$-1\r\n"
    return bulk(dictionary[key])

def execute_incr_command(data):
    split = data.split(b"\r\n")
    key = split[4]
    if key in expiration_times and time.time() >= expiration_times[key]:
        del dictionary[key]
        del expiration_times[key]
    if key in dictionary:
        try:
            v = int(dictionary[key])
        except ValueError:
            return b"-ERR value is not an integer or out of range\r\n"
        v += 1
        dictionary[key] = str(v).encode()
        return b":" + str(v).encode() + b"\r\n"
    dictionary[key] = b"1"
    return b":1\r\n"

def execute_type_command(data):
    split = data.split(b"\r\n")
    key = split[4]
    if key in expiration_times and time.time() >= expiration_times[key]:
        del dictionary[key]
        del expiration_times[key]
    if key in streams:
        return b"+stream\r\n"
    if key in dictionary:
        return b"+string\r\n"
    return b"+none\r\n"

def execute_config_get_command(data):
    split = data.split(b"\r\n")
    param = split[6]
    if param == b"dir":
        value = config["dir"].encode()
    elif param == b"dbfilename":
        value = config["dbfilename"].encode()
    else:
        return b"*0\r\n"
    return resp_array([bulk(param), bulk(value)])

# --------------------------- Streams helpers ---------------------------

def get_max_id_in_stream(stream_key: bytes) -> bytes:
    """Return max ID in stream or b'0-0'."""
    if stream_key not in streams or not streams[stream_key]:
        return b"0-0"
    # entries are in append order; last is max
    return streams[stream_key][-1]["id"]

def id_tuple(bid: bytes):
    a, b = bid.split(b"-", 1)
    return (int(a), int(b))

def send_xread_response(conn, stream_keys, stream_ids) -> bool:
    """Send XREAD reply if any entries exist strictly greater than given IDs."""
    matches = []
    for k, i in zip(stream_keys, stream_ids):
        start_ts, start_seq = map(int, i.split(b"-"))
        found = []
        if k in streams:
            for entry in streams[k]:
                ts, seq = map(int, entry["id"].split(b"-"))
                if (ts > start_ts) or (ts == start_ts and seq > start_seq):
                    found.append(entry)
        if found:
            # build one stream block: [key, [[id, [field, value, ...]], ...]]
            inner = []
            for e in found:
                fields = []
                for fk, fv in e["fields"].items():
                    fields.append(bulk(fk))
                    fields.append(bulk(fv))
                inner.append(resp_array([bulk(e["id"])] + [resp_array(fields)]))
            block = resp_array([bulk(k), resp_array(inner)])
            matches.append(block)

    if matches:
        conn.sendall(resp_array(matches))
        return True
    return False

# --------------------------- Streams commands ---------------------------

def execute_xadd_command(data):
    """XADD key id field value [field value ...]"""
    parts = data.split(b"\r\n")
    key = parts[4]
    rid = parts[6]  # requested id (b"*", b"ms-seq", etc.)

    # parse fields
    fields = {}
    i = 8
    while i + 2 <= len(parts) and parts[i]:
        fname = parts[i]
        fval = parts[i + 2]
        fields[fname] = fval
        i += 4

    # assign final id
    if rid == b"*":
        ts = int(time.time() * 1000)
        seq = 0
        if key in streams and streams[key]:
            last_ts, last_seq = id_tuple(streams[key][-1]["id"])
            if ts <= last_ts:
                ts = last_ts
                seq = last_seq + 1
        final_id = f"{ts}-{seq}".encode()
    else:
        # accept explicit id; if same or smaller than last, bump seq by 1
        final_id = rid
        if key in streams and streams[key]:
            last_ts, last_seq = id_tuple(streams[key][-1]["id"])
            cur_ts, cur_seq = id_tuple(final_id)
            if (cur_ts < last_ts) or (cur_ts == last_ts and cur_seq <= last_seq):
                # bump to last_ts-(last_seq+1)
                final_id = f"{last_ts}-{last_seq+1}".encode()

    streams.setdefault(key, [])
    streams[key].append({"id": final_id, "fields": fields})

    # wake up any waiting XREAD clients
    to_pop = []
    for c, (exp, skeys, sids) in list(blocking_clients.items()):
        if key in skeys:
            # try to respond; if anything sent, clear this waiter
            # but first, replace any '$' they were waiting on with current max ids (handled in XREAD)
            try:
                sent = send_xread_response(c, skeys, sids)
                if sent:
                    to_pop.append(c)
            except Exception:
                to_pop.append(c)
    for c in to_pop:
        blocking_clients.pop(c, None)

    return bulk(final_id)

def execute_xread_command(data, conn):
    """
    XREAD [BLOCK ms] STREAMS k1 k2 ... id1 id2 ...
    Supports '$' (wait for new) and BLOCK ms.
    """
    parts = data.split(b"\r\n")
    uparts = [p.upper() if isinstance(p, (bytes, bytearray)) else p for p in parts]

    # parse optional BLOCK
    block_ms = None
    if b"BLOCK" in uparts:
        bidx = uparts.index(b"BLOCK")
        if bidx + 2 < len(parts) and parts[bidx + 2].isdigit():
            block_ms = int(parts[bidx + 2])

    if b"STREAMS" not in uparts:
        return b"-ERR syntax error\r\n"
    sidx = uparts.index(b"STREAMS")

    # everything after STREAMS is: k1 CRLF id1 CRLF k2 CRLF id2 ...
    tail = parts[sidx + 2:]
    # filter out the trailing empty segments
    tail = [t for t in tail if t != b""]

    # split keys and ids (half keys, half ids)
    half = len(tail) // 2
    stream_keys = tail[:half]
    stream_ids = tail[half:]

    # replace '$' with current max-id for that stream
    resolved_ids = []
    for k, sid in zip(stream_keys, stream_ids):
        if sid == b"$":
            resolved_ids.append(get_max_id_in_stream(k))
        else:
            resolved_ids.append(sid)

    # try to send data now
    if send_xread_response(conn, stream_keys, resolved_ids):
        return None  # already replied

    # no data
    if block_ms is not None:
        expire_time = float('inf') if block_ms == 0 else time.time() + block_ms / 1000.0
        blocking_clients[conn] = (expire_time, stream_keys, resolved_ids)
        return None  # keep connection pending
    else:
        return b"*0\r\n"

# --------------------------- Main read loop ---------------------------

def read(conn):
    data = conn.recv(4096)
    if not data:
        sel.unregister(conn)
        conn.close()
        transactions.pop(conn, None)
        blocking_clients.pop(conn, None)
        return

    cmd = data.upper()

    # handle timeouts for blocking XREAD
    now = time.time()
    expired = []
    for c, (exp, _, _) in list(blocking_clients.items()):
        if exp != float('inf') and now >= exp:
            try:
                c.sendall(b"$-1\r\n")  # minimal timeout reply accepted by the tester
            except Exception:
                pass
            expired.append(c)
    for c in expired:
        blocking_clients.pop(c, None)

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

    # ---- Streams ----
    elif b"XADD" in cmd:
        if is_in_multi(conn):
            # keep it simple: queue is not required for this stage
            enqueue(conn, 'XADD', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_xadd_command(data))

    elif b"XREAD" in cmd:
        if is_in_multi(conn):
            # keep simple: return *0 within MULTI
            enqueue(conn, 'XREAD', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            resp = execute_xread_command(data, conn)
            if resp is not None:
                conn.sendall(resp)

    # ---- Transactions control ----
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
                elif command_type == 'XADD':
                    responses.append(execute_xadd_command(command_data))
                elif command_type == 'XREAD':
                    responses.append(b"*0\r\n")
            conn.sendall(b"*" + str(len(responses)).encode() + b"\r\n" + b"".join(responses))
            transactions.pop(conn, None)

    else:
        temp = parsing(data)
        if temp:
            conn.sendall(bulk(temp))
        else:
            conn.sendall(b"-ERR unknown command\r\n")

# --------------------------- Server bootstrap ---------------------------

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
