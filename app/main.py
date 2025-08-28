import socket
import selectors
import time
import sys
import os

sel = selectors.DefaultSelector()
dictionary = {}
expiration_times = {}  # New: track expiration times separately
streams = {}
blocking_clients = {}  # Store blocking XREAD clients
# --- MINIMAL CHANGE: per-connection transactions instead of global flags ---
transactions = {}  # conn -> {"in_multi": bool, "queue": [(cmd, data), ...]}
config = {
    'dir': '/tmp',
    'dbfilename': 'dump.rdb'
}

def load_rdb():
    """Minimal RDB loader: handles multiple keys, both with and without 0x00 type markers."""
    global dictionary
    dictionary.clear()

    rdb_path = os.path.join(config['dir'], config['dbfilename'])
    if not os.path.exists(rdb_path):
        return

    try:
        with open(rdb_path, 'rb') as f:
            data = f.read()

        i = 0
        # Skip header
        if data[:5] == b"REDIS":
            i = 9

        while i < len(data):
            b = data[i]

            # End of file
            if b == 0xFF:
                break

            # Skip metadata
            if b == 0xFA:
                name_len = data[i + 1]
                i += 2 + name_len
                val_len = data[i]
                i += 1 + val_len
                continue

            # Skip database selector
            if b == 0xFE:
                i += 2
                continue

            # Skip hash table size info
            if b == 0xFB:
                i += 3
                continue

            # Case 1: 0x00 marker before key
            if b == 0x00:
                key_len = data[i + 1]
                key = data[i + 2:i + 2 + key_len]
                i = i + 2 + key_len

                val_len = data[i]
                value = data[i + 1:i + 1 + val_len]
                i = i + 1 + val_len

                dictionary[key] = value
                continue

            # Case 2: No 0x00 marker → length-prefixed key
            if 1 <= b <= 0x40:  # plausible key length
                key_len = b
                key = data[i + 1:i + 1 + key_len]
                i = i + 1 + key_len

                val_len = data[i]
                value = data[i + 1:i + 1 + val_len]
                i = i + 1 + val_len

                dictionary[key] = value
                continue

            # Unknown byte → skip it
            i += 1

    except Exception:
        dictionary.clear()


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
    """Get the maximum ID currently in the stream, or '0-0' if stream is empty or doesn't exist"""
    if stream_key not in streams or not streams[stream_key]:
        return b"0-0"
    max_entry = max(streams[stream_key], key=lambda entry: tuple(map(int, entry['id'].split(b'-'))))
    return max_entry['id']

# --- MINIMAL ADD: helpers for per-connection transaction state ---
def is_in_multi(conn):
    return conn in transactions and transactions[conn]["in_multi"]

def enqueue(conn, cmd, data):
    transactions.setdefault(conn, {"in_multi": True, "queue": []})
    transactions[conn]["queue"].append((cmd, data))

def execute_keys_command(data):
    """Handle KEYS * command"""
    keys = list(dictionary.keys())
    result = b"*" + str(len(keys)).encode() + b"\r\n"
    for key in keys:
        result += string(key)
    return result

# Command execution functions
def execute_set_command(data):
    global dictionary, expiration_times
    split = data.split(b"\r\n")
    key = split[4]
    value = split[6]
    dictionary[key] = value
    if len(split) > 10 and split[10].isdigit():
        expiration_times[key] = time.time() + int(split[10])/1000
    else:
        if key in expiration_times:
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
    value = dictionary[key]
    return string(value)

def execute_incr_command(data):
    global dictionary, expiration_times
    split = data.split(b"\r\n")
    key = split[4]
    if key in expiration_times and time.time() >= expiration_times[key]:
        del dictionary[key]
        del expiration_times[key]
    print(f"INCR DEBUG: key={key}, before: dictionary={dictionary}")  # Debug line
    if key in dictionary:
        try:
            current_value = int(dictionary[key])
            new_value = current_value + 1
            dictionary[key] = str(new_value).encode()
            print(f"INCR DEBUG: key={key}, after: dictionary={dictionary}")  # Debug line
            return b":" + str(new_value).encode() + b"\r\n"
        except ValueError:
            return b"-ERR value is not an integer or out of range\r\n"
    else:
        dictionary[key] = b"1"
        print(f"INCR DEBUG: key={key}, new key, after: dictionary={dictionary}")  # Debug line
        return b":1\r\n"

def execute_type_command(data):
    global streams, dictionary, expiration_times
    split = data.split(b"\r\n")
    key = split[4]
    if key in expiration_times and time.time() >= expiration_times[key]:
        del dictionary[key]
        del expiration_times[key]
    if key in streams:
        return b'+stream\r\n'
    elif key in dictionary:
        return b'+string\r\n'
    else:
        return b"+none\r\n"
    
def execute_config_get_command(data):
    split = data.split(b"\r\n")
    param = split[6]
    
    if param == b'dir':
        value = config['dir'].encode()
        result = b"*2\r\n"
        result += string(param)
        result += string(value)
        return result
    elif param == b'dbfilename':
        value = config['dbfilename'].encode()
        result = b"*2\r\n"
        result += string(param)
        result += string(value)
        return result
    else:
        return b"*0\r\n"

def execute_xadd_command(data):
    global streams, blocking_clients
    split = data.split(b"\r\n")
    key = split[4]
    value = split[6]

    if value == b"*":
        curr_timestamps = int(time.time() * 1000)
        carry = 0
        if key in streams and streams[key]:
            final_entry = streams[key][-1]
            parts = final_entry['id'].split(b"-")
            timestamp = int(parts[0])
            if curr_timestamps == timestamp:
                carry = int(parts[1]) + 1
            elif curr_timestamps < timestamp:
                curr_timestamps = timestamp
                carry = int(parts[1]) + 1
        value = str(curr_timestamps).encode() + b"-" + str(carry).encode()

    if b"*" in value:
        divide = value.split(b"-")
        if divide[1] == b"*":
            last = -1
            if key in streams and streams[key]:
                for enter in streams[key]:
                    a, b = enter['id'].split(b"-")
                    if a == divide[0]:
                        last = max(last, int(b))
            if divide[0] == b"0" and last == -1:
                New = 1
            else:
                New = last + 1
            value = divide[0] + b"-" + str(New).encode()

    comp_ts, comp_seq = map(int, value.split(b"-"))
    if comp_ts == 0 and comp_seq == 0:
        return b"-ERR The ID specified in XADD must be greater than 0-0\r\n"

    if key in streams and streams[key]:
        last = streams[key][-1]
        temp_ts, temp_seq = map(int, last['id'].split(b"-"))
        if comp_ts < temp_ts or (comp_ts == temp_ts and comp_seq <= temp_seq):
            return b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"

    fields = {}
    for i in range(8, len(split), 4):
        if i + 2 < len(split) and split[i]:
            fname = split[i]
            fval = split[i + 2]
            fields[fname] = fval

    if key not in streams:
        streams[key] = []

    entry = {'id': value, 'fields': fields}
    streams[key].append(entry)

    # Handle blocking clients
    clients_to_remove = []
    for client_conn, block_info in list(blocking_clients.items()):
        expire_time, stream_keys, stream_ids = block_info
        if key in stream_keys:
            send_xread_response(client_conn, stream_keys, stream_ids)
            clients_to_remove.append(client_conn)

    for client_conn in clients_to_remove:
        if client_conn in blocking_clients:
            del blocking_clients[client_conn]

    return string(value)

def execute_xrange_command(data):
    global streams
    xrange_split = data.split(b"\r\n")
    xrange_var = xrange_split[4]
    xrange_start = xrange_split[6]
    xrange_end = xrange_split[8]

    if b"-" not in xrange_start:
        xrange_start = xrange_start + b"-0"
    if xrange_end == b"+":
        xrange_end = b"9999999999999-9999999999999"
    elif b"-" not in xrange_end:
        xrange_end = xrange_end + b"-9999999999999"

    entries = []
    if xrange_var in streams and streams[xrange_var]:
        for p in streams[xrange_var]:
            id = p['id']
            if id >= xrange_start and id <= xrange_end:
                entries.append(p)

    result = b"*" + str(len(entries)).encode() + b"\r\n"
    for p in entries:
        result += b"*2\r\n"
        result += string(p['id'])
        fields_count = len(p['fields']) * 2
        result += b"*" + str(fields_count).encode() + b"\r\n"
        for fkey, fval in p['fields'].items():
            result += string(fkey)
            result += string(fval)
    return result

def execute_xread_command(data, conn):
    """Special case for XREAD since it needs the connection object"""
    global streams, blocking_clients
    xread_split = data.split(b"\r\n")
    upper_parts = [p.upper() for p in xread_split]

    is_blocking = b"BLOCK" in upper_parts

    try:
        streams_idx = upper_parts.index(b"STREAMS")
        num_streams = (len(xread_split) - streams_idx - 2) // 4
        keys_start = streams_idx + 2

        stream_keys = [xread_split[keys_start + i * 2] for i in range(num_streams)]
        ids_start = keys_start + num_streams * 2
        stream_ids = [xread_split[ids_start + i * 2] for i in range(num_streams)]

        # Handle $ replacement - replace $ with max ID in each stream
        processed_stream_ids = []
        for i, (stream_key, stream_id) in enumerate(zip(stream_keys, stream_ids)):
            if stream_id == b"$":
                max_id = get_max_id_in_stream(stream_key)
                processed_stream_ids.append(max_id)
            else:
                processed_stream_ids.append(stream_id)

        stream_ids = processed_stream_ids

    except (ValueError, IndexError):
        return b"-ERR syntax error\r\n"

    # This function now only sends data if it finds something.
    data_was_sent = send_xread_response(conn, stream_keys, stream_ids)

    if not data_was_sent:
        if is_blocking:
            block_idx = upper_parts.index(b"BLOCK")
            timeout_ms = int(xread_split[block_idx + 2])
            expire_time = float('inf') if timeout_ms == 0 else time.time() + (timeout_ms / 1000.0)
            blocking_clients[conn] = (expire_time, stream_keys, stream_ids)
            return None  # Don't send response, client is blocking
        else:
            return b"*0\r\n"
    
    return None  # Response already sent by send_xread_response

def read(conn):
    global dictionary, streams
    data = conn.recv(1024)
    if not data:
        sel.unregister(conn)
        conn.close()
        if conn in blocking_clients:
            del blocking_clients[conn]
        if conn in transactions:        # MINIMAL ADD: cleanup per-conn txn state
            del transactions[conn]
        return

    if b"PING" in data.upper():
        conn.sendall(b"+PONG\r\n")
    
    elif b"KEYS" in data.upper():
        response = execute_keys_command(data)
        conn.sendall(response)
        
    elif b"CONFIG" in data.upper() and b"GET" in data.upper():
        response = execute_config_get_command(data)
        conn.sendall(response)

    elif b"SET" in data.upper():
        if is_in_multi(conn):
            enqueue(conn, 'SET', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            response = execute_set_command(data)
            conn.sendall(response)

    elif b"GET" in data.upper():
        if is_in_multi(conn):
            enqueue(conn, 'GET', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            response = execute_get_command(data)
            conn.sendall(response)

    elif b"XADD" in data.upper():
        if is_in_multi(conn):
            enqueue(conn, 'XADD', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            response = execute_xadd_command(data)
            if response:  # Check if there's an error response
                conn.sendall(response)

    elif b"XRANGE" in data.upper():
        if is_in_multi(conn):
            enqueue(conn, 'XRANGE', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            response = execute_xrange_command(data)
            conn.sendall(response)

    elif b"XREAD" in data.upper():
        if is_in_multi(conn):
            enqueue(conn, 'XREAD', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            response = execute_xread_command(data, conn)
            if response:  # Only send if there's a response (not blocking)
                conn.sendall(response)
                

    elif b"INCR" in data.upper():
        if is_in_multi(conn):
            enqueue(conn, 'INCR', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            response = execute_incr_command(data)
            conn.sendall(response)

    elif b"TYPE" in data.upper():
        if is_in_multi(conn):
            enqueue(conn, 'TYPE', data)
            conn.sendall(b"+QUEUED\r\n")
        else:
            response = execute_type_command(data)
            conn.sendall(response)
    
    elif b"MULTI" in data.upper():
        transactions[conn] = {"in_multi": True, "queue": []}  # MINIMAL CHANGE
        conn.sendall(b"+OK\r\n")
    
    elif b"DISCARD" in data.upper():
        if is_in_multi(conn):
            del transactions[conn]
            conn.sendall(b"+OK\r\n")
        else:
            conn.sendall(b"-ERR DISCARD without MULTI\r\n")
            
    
    elif b"EXEC" in data.upper():
        if is_in_multi(conn):  # MINIMAL CHANGE: use per-conn state
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
                elif command_type == 'XRANGE':
                    responses.append(execute_xrange_command(command_data))
                elif command_type == 'XREAD':
                    responses.append(b"*0\r\n")  # keep simple, non-blocking in MULTI
            
            # Send array response (keep your original formatting)
            result = b"*" + str(len(responses)).encode() + b"\r\n"
            for response in responses:
                result += response
            
            conn.sendall(result)
            
            # Reset transaction state ONLY for this connection (minimal change)
            del transactions[conn]
        else:
            conn.sendall(b"-ERR EXEC without MULTI\r\n")

    elif data is not None:
        temp = parsing(data)
        if temp:
            res = string(temp)
            conn.sendall(res)
        else:
            conn.sendall(b"-ERR unknown command\r\n")
    else:
        conn.sendall(b"-ERR unknown command\r\n")

# This helper function is modified to only send a response if data is found.
def send_xread_response(conn, stream_keys, stream_ids):
    matches = []
    for k, i in zip(stream_keys, stream_ids):
        start_ts, start_seq = map(int, i.split(b'-'))
        matched = []
        if k in streams:
            for entry in streams[k]:
                entry_ts, entry_seq = map(int, entry["id"].split(b"-"))
                if (entry_ts > start_ts) or (entry_ts == start_ts and entry_seq > start_seq):
                    matched.append(entry)
        
        if matched:
            stream_result = b"*2\r\n" + string(k)
            stream_result += b"*" + str(len(matched)).encode() + b"\r\n"
            for entry in matched:
                stream_result += b"*2\r\n" + string(entry['id'])
                fields = entry["fields"]
                stream_result += b"*" + str(len(fields) * 2).encode() + b"\r\n"
                for fk, fv in fields.items():
                    stream_result += string(fk) + string(fv)
            matches.append(stream_result)

    if matches:
        result = b"*" + str(len(matches)).encode() + b"\r\n" + b"".join(matches)
        if conn: conn.sendall(result)
        return True # Data was found and sent
    
    return False # No data was found

def main(port = 6379):
    # Load RDB file at startup
    load_rdb()
    
    server_socket = socket.create_server(("localhost", port ), reuse_port=True)
    server_socket.setblocking(False)
    sel.register(server_socket, selectors.EVENT_READ, accept)
    while True:
        events = sel.select(timeout=0.1)
        
        for key, _ in events:
            callback = key.data
            callback(key.fileobj)

        now = time.time()
        clients_to_remove = []
        for client_conn, (expire_time, _, _) in list(blocking_clients.items()):
            if now >= expire_time:
                try:
                    client_conn.sendall(b"$-1\r\n")
                except:
                    pass
                clients_to_remove.append(client_conn)

        for client_conn in clients_to_remove:
            if client_conn in blocking_clients:
                del blocking_clients[client_conn]


if __name__ == "__main__":
    port = 6379
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])
    if "--dir" in sys.argv:
        idx = sys.argv.index("--dir")
        if idx + 1 < len(sys.argv):
            config['dir'] = sys.argv[idx + 1]
    
    if "--dbfilename" in sys.argv:
        idx = sys.argv.index("--dbfilename")
        if idx + 1 < len(sys.argv):
            config['dbfilename'] = sys.argv[idx + 1]
    main(port)