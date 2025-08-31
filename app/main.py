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
lists = {}

config = {
    'dir': '/tmp',
    'dbfilename': 'dump.rdb'
}


def load_rdb():
    """Load RDB keys, values, and expirations."""
    global dictionary, expiration_times
    dictionary.clear()
    expiration_times.clear()

    path = os.path.join(config['dir'], config['dbfilename'])
    if not os.path.exists(path):
        return

    data = open(path, "rb").read()
    i = 0

    # Skip header
    if data.startswith(b"REDIS"):
        i = 9

    # Move to main table marker (0xFB)
    while i < len(data) and data[i] != 0xFB:
        i += 1
    i += 3  # skip 0xFB + 2 size bytes

    # Loop over entries
    while i < len(data):
        if data[i] == 0xFF:  # end of file
            break

        expire_ts = None

        # Expiry in seconds (0xFD)
        if data[i] == 0xFD:
            expire_ts = int.from_bytes(data[i+1:i+5], "little") * 1000
            i += 5
        # Expiry in ms (0xFC)
        elif data[i] == 0xFC:
            expire_ts = int.from_bytes(data[i+1:i+9], "little")
            i += 9

        # Type byte (only handle strings = 0x00)
        type_byte = data[i]
        i += 1
        if type_byte != 0x00:
            break

        # Key
        key_len = data[i]
        i += 1
        key = data[i:i+key_len]
        i += key_len

        # Value
        val_len = data[i]
        i += 1
        val = data[i:i+val_len]
        i += val_len

        # Skip expired keys
        if expire_ts and expire_ts / 1000.0 <= time.time():
            continue

        # Store in memory
        dictionary[key] = val
        if expire_ts:
            expiration_times[key] = expire_ts / 1000.0


def parsing(data):
    split = data.split(b"\r\n")
    if len(split) > 4 and split[2] == b"ECHO":
        return split[4]
    return None


def parse_resp_array(data):
    """Parse RESP array format and return list of arguments."""
    parts = data.split(b"\r\n")
    args = []
    i = 0
    
    # Skip the array header (*N)
    if i < len(parts) and parts[i].startswith(b"*"):
        i += 1
    
    while i < len(parts):
        if parts[i].startswith(b"$") and i + 1 < len(parts):
            # Bulk string: $length\r\ndata\r\n
            args.append(parts[i + 1])
            i += 2
        elif parts[i] == b"":
            i += 1
        else:
            i += 1
    
    return args


def string(words):
    return b"$" + str(len(words)).encode() + b"\r\n" + words + b"\r\n"


def accept(sock):
    conn, _ = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)


def get_max_id_in_stream(stream_key):
    if stream_key not in streams or not streams[stream_key]:
        return b"0-0"
    max_entry = max(streams[stream_key], key=lambda e: tuple(map(int, e['id'].split(b'-'))))
    return max_entry['id']

def generate_next_id(stream_key, raw_id=None):
    # Fully auto-generated ID "*"
    if raw_id is None or raw_id == b"*":
        ms = int(time.time() * 1000)
        existing = [e for e in streams.get(stream_key, []) if int(e["id"].split(b"-")[0]) == ms]
        if existing:
            seq = max(int(e["id"].split(b"-")[1]) for e in existing) + 1
        else:
            seq = 0
        return f"{ms}-{seq}".encode()

    # Partially auto-generated ID "ms-*"
    if raw_id.endswith(b"-*"):
        ms = int(raw_id.split(b"-")[0])
        existing = [e for e in streams.get(stream_key, []) if int(e["id"].split(b"-")[0]) == ms]
        if existing:
            seq = max(int(e["id"].split(b"-")[1]) for e in existing) + 1
        else:
            # Special rule: if ms == 0, first ID is "0-1"
            seq = 1 if ms == 0 else 0
        return f"{ms}-{seq}".encode()

    # Fixed ID
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


def notify_blocked_clients(stream_key):
    """Check if any blocked clients should be notified about new entries."""
    clients_to_remove = []
    
    for conn, (expire_time, stream_keys, resolved_ids) in blocking_clients.items():
        if stream_key in stream_keys:
            # Check if there are new entries for this client
            has_new_entries = False
            for key, last_id in zip(stream_keys, resolved_ids):
                if key == stream_key:
                    for entry in streams.get(key, []):
                        if compare_ids(entry["id"], last_id) > 0:
                            has_new_entries = True
                            break
                    break
            
            if has_new_entries:
                # Send response to blocked client
                response = build_xread_response(stream_keys, resolved_ids)
                try:
                    conn.sendall(response)
                    clients_to_remove.append(conn)
                except:
                    # Connection might be closed
                    clients_to_remove.append(conn)
    
    # Remove notified clients from blocking list
    for conn in clients_to_remove:
        blocking_clients.pop(conn, None)


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
    args = parse_resp_array(data)
    if len(args) < 4:
        return b"-ERR wrong number of arguments\r\n"
    
    stream_key = args[1]
    raw_id = args[2]

    # Generate entry ID
    if raw_id == b"*":
        entry_id = generate_next_id(stream_key)
    elif raw_id.endswith(b"-*"):
        entry_id = generate_next_id(stream_key, raw_id)
    else:
        entry_id = raw_id

    # Parse entry_id into ms and seq
    new_ms, new_seq = map(int, entry_id.split(b"-"))

    # ✅ Special check: Redis forbids 0-0
    if new_ms == 0 and new_seq == 0:
        return b"-ERR The ID specified in XADD must be greater than 0-0\r\n"

    # Validate strictly increasing IDs
    if stream_key in streams and streams[stream_key]:
        last_id = streams[stream_key][-1]["id"]
        last_ms, last_seq = map(int, last_id.split(b"-"))

        if new_ms < last_ms or (new_ms == last_ms and new_seq <= last_seq):
            return b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"

    # Parse fields
    fields = {}
    for i in range(3, len(args), 2):
        if i + 1 < len(args):
            fields[args[i]] = args[i + 1]

    if stream_key not in streams:
        streams[stream_key] = []

    entry = {"id": entry_id, "fields": fields}
    streams[stream_key].append(entry)

    notify_blocked_clients(stream_key)

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

def execute_RPUSH_command(data):
    global lists
    split = data.split(b"\r\n")
    key = split[4]
    values = [split[i] for i in range(6, len(split) - 1, 2)]
    if key in lists:
        lists[key].extend(values)
    else:
        lists[key] = values
    length = len(lists[key])
    return b":" + str(length).encode() + b"\r\n"

def execute_LPUSH_command(data):
    global lists
    split = data.split(b"\r\n")
    key = split[4]
    values = [split[i] for i in range(6, len(split) - 1, 2)]
    if key in lists:
        lists[key] = list(reversed(values)) + lists[key]
    else:
        lists[key] = list(reversed(values))
    length = len(lists[key])
    return b":" + str(length).encode() + b"\r\n"

def execute_LRANGE_command(data):
    global lists
    split = data.split(b"\r\n")
    key = split[4]
    start = int(split[6])
    end = int(split[8])
    if key not in lists:
        return b"*0\r\n"
    values = lists[key]
    if start < 0:
        start += len(values)
    if end < 0:
        end += len(values)
    start = max(0, start)
    end = min(end, len(values) - 1)
    
    if start > end or start >= len(values):
        return b"*0\r\n"

    result = values[start:end + 1]
    return b"*" + str(len(result)).encode() + b"\r\n" + b"".join(string(v) for v in result)
    # if key in lists:
    #     values = lists[key][start:end + 1]
    #     return b"*" + str(len(values)).encode() + b"\r\n" + b"".join(string(v) for v in values)
    # return b"*0\r\n"
    
def execute_LLEN_command(data):
    split = data.split(b"\r\n")
    key = split[4]
    if key not in lists:
        return b":0\r\n"
    length = len(lists[key])
    return b":" + str(length).encode() + b"\r\n"

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
    for conn, (expire_time, stream_keys, resolved_ids) in blocking_clients.items():
        if current_time >= expire_time:
            try:
                conn.sendall(b"*-1\r\n")
            except:
                pass  # Connection might be closed
            expired_clients.append(conn)
    for conn in expired_clients:
        blocking_clients.pop(conn, None)


def read(conn):
    global dictionary, streams, lists
    try:
        data = conn.recv(1024)
        if not data:
            sel.unregister(conn)
            conn.close()
            transactions.pop(conn, None)
            blocking_clients.pop(conn, None)
            return
    except:
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
    elif b"LRANGE" in cmd:
        if is_in_multi(conn):
            enqueue(conn, 'LRANGE', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_LRANGE_command(data))
    elif b"LLEN" in cmd:
        if is_in_multi(conn):
            enqueue(conn, 'LLEN', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_LLEN_command(data))
    elif b"LPUSH" in cmd:
        if is_in_multi(conn):
            enqueue(conn, 'LPUSH', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_LPUSH_command(data))
    elif b"INCR" in cmd:
        if is_in_multi(conn):
            enqueue(conn, 'INCR', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_incr_command(data))
    elif b"RPUSH" in cmd:
        if is_in_multi(conn):
            enqueue(conn, 'RPUSH', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            conn.sendall(execute_RPUSH_command(data))
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