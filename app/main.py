import socket
import selectors
import time

sel = selectors.DefaultSelector()
dictionary, temp1, temp2, temp3 = {}, b"", b"", None
streams = {}
blocking_clients = {}  # Store blocking XREAD clients

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
    
    # Find the entry with the maximum ID
    max_entry = max(streams[stream_key], key=lambda entry: tuple(map(int, entry['id'].split(b'-'))))
    return max_entry['id']

def read(conn):
    global dictionary, temp3, temp1, temp2, streams
    data = conn.recv(1024)
    if not data:
        sel.unregister(conn)
        conn.close()
        if conn in blocking_clients:
            del blocking_clients[conn]
        return

    if b"PING" in data.upper():
        conn.sendall(b"+PONG\r\n")

    elif b"SET" in data.upper():
        conn.sendall(b"+OK\r\n")
        split = data.split(b"\r\n")

        temp1 = split[4]
        temp2 = split[6]
        dictionary[temp1] = temp2

        if len(split) > 10 and split[10].isdigit():
            temp3 = time.time() + int(split[10])/1000
        else:
            temp3 = None

    elif b"XADD" in data.upper():
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
            conn.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
            return

        if key in streams and streams[key]:
            last = streams[key][-1]
            temp_ts, temp_seq = map(int, last['id'].split(b"-"))
            if comp_ts < temp_ts or (comp_ts == temp_ts and comp_seq <= temp_seq):
                conn.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                return

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
        conn.sendall(string(value))

        clients_to_remove = []
        for client_conn, block_info in list(blocking_clients.items()):
            expire_time, stream_keys, stream_ids = block_info
            if key in stream_keys:
                send_xread_response(client_conn, stream_keys, stream_ids)
                clients_to_remove.append(client_conn)
        
        for client_conn in clients_to_remove:
            if client_conn in blocking_clients:
                del blocking_clients[client_conn]

    elif b"XRANGE" in data.upper():
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
        conn.sendall(result)

    elif b"XREAD" in data.upper():
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
                    # Replace $ with the maximum ID currently in the stream
                    max_id = get_max_id_in_stream(stream_key)
                    processed_stream_ids.append(max_id)
                else:
                    processed_stream_ids.append(stream_id)
            
            stream_ids = processed_stream_ids
            
        except (ValueError, IndexError):
            conn.sendall(b"-ERR syntax error\r\n")
            return
        
        # This function now only sends data if it finds something.
        data_was_sent = send_xread_response(conn, stream_keys, stream_ids)
        
        # This block now correctly handles the logic after the check.
        if not data_was_sent:
            if is_blocking:
                # If it's a blocking call and no data was sent, we just wait.
                # The client is registered to be unblocked later.
                block_idx = upper_parts.index(b"BLOCK")
                timeout_ms = int(xread_split[block_idx + 2])
                expire_time = float('inf') if timeout_ms == 0 else time.time() + (timeout_ms / 1000.0)
                blocking_clients[conn] = (expire_time, stream_keys, stream_ids)
            else:
                # If it's NOT a blocking call, we send an empty array now.
                conn.sendall(b"*0\r\n")

    elif b"INCR" in data.upper():
        split = data.split(b"\r\n")
        temp = split[4]
        if temp in dictionary:
            res = int(dictionary[temp])
            dictionary[temp] = str(res + 1).encode()
            ret = b":" + dictionary[temp] + b"\r\n"
            conn.sendall(ret)
        else:
            conn.sendall(b":1\r\n")
            dictionary[temp] = 1

    elif b"TYPE" in data.upper():
        split = data.split(b"\r\n")
        if split[4] in streams:
            conn.sendall(b'+stream\r\n')
        elif split[4] in dictionary:
            conn.sendall(b'+string\r\n')
        else:
            conn.sendall(b"+none\r\n")

    elif b"GET" in data.upper():
        split = data.split(b"\r\n")
        if temp1 == split[4] and (temp3 is None or time.time() < temp3):
            res = string(temp2)
            conn.sendall(res)
        else:
            conn.sendall(b"$-1\r\n")
    elif data is not None:
        temp = parsing(data)
        res = string(temp)
        conn.sendall(res)
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


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
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
    main()
    
        # xread_var = xread_split[6]
        # xread_time = xread_split[8]
        
        # if b"-" not in xread_time:
        #     xread_time = xread_time + b"-0"
        
        # matched = []
        
        # if xread_var in streams and streams[xread_var]:
        #     for p in streams[xread_var]:
        #         id = p['id']
        #         if id > xread_time:
        #             matched.append(p)
                
        # if matched:
        #     result = b"*1\r\n"
        #     result += b"*2\r\n"
        #     result += string(xread_var)
        #     result += b"*" + str(len(matched)).encode() + b"\r\n"
            
        #     for q in matched:
        #         result += b"*2\r\n"
        #         result += string(q['id'])
                
        #         count = len(q['fields']) * 2
        #         result += b"*" + str(count).encode() + b"\r\n"
                
        #         for fkey, fval in q['fields'].items():
        #             result += string(fkey)
        #             result += string(fval)
        # else:
        #     result = b"*0\r\n"
        # conn.sendall(result)

# import socket  # noqa: F401
# import threading

# def handle_client(connection):
#     while True:
#         data = connection.recv(1024)
#         if b"PING" in data.upper():
#             connection.sendall(b"+PONG\r\n")
    

# def main():
#     server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
#     while True:
#         connection, _ = server_socket.accept() 
#         thread = threading.Thread(target=handle_client, args=(connection,))
#         thread.start()


# if __name__ == "__main__":
#     main()
