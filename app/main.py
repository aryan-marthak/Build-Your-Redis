import socket
import selectors
import time

sel = selectors.DefaultSelector()
dictionary, temp1, temp2, temp3 = {}, b"", b"", None
streams = {}
blocking_clients = {}  # Store blocking XREAD clients

def parsing(data):
    split = data.split(b"\r\n")
    if len(split) > 4 and split[2].upper() == b"ECHO":
        return split[4]
    return None

def string(words):
    return b"$" + str(len(words)).encode() + b"\r\n" + words + b"\r\n"

def accept(sock):
    conn, _ = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

def read(conn):
    global dictionary, temp3, temp1, temp2, streams
    data = conn.recv(1024)
    if not data:
        sel.unregister(conn)
        conn.close()
        if conn in blocking_clients:
            del blocking_clients[conn]
        return

    command_parts = data.split(b"\r\n")
    command = command_parts[2].upper()

    if command == b"PING":
        conn.sendall(b"+PONG\r\n")

    elif command == b"SET":
        conn.sendall(b"+OK\r\n")
        temp1 = command_parts[4]
        temp2 = command_parts[6]
        dictionary[temp1] = temp2
        
        if len(command_parts) > 10 and command_parts[8].upper() == b'PX' and command_parts[10].isdigit():
            temp3 = time.time() + int(command_parts[10])/1000
        else:
            temp3 = None

    elif command == b"XADD":
        key = command_parts[4]
        value = command_parts[6]
        
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
        elif b"*" in value:
            divide = value.split(b"-")
            if divide[1] == b"*":
                last = -1
                if key in streams and streams[key]:
                    for enter in streams[key]:
                        a, b = enter['id'].split(b"-")
                        if a == divide[0]:
                            last = max(last, int(b))
                
                New = last + 1 if int(divide[0]) > 0 or last != -1 else 1
                value = divide[0] + b"-" + str(New).encode()

        comp = value.split(b"-")
        if int(comp[0]) == 0 and int(comp[1]) == 0:
            conn.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
            return
        
        if key in streams and streams[key]:
            last_id_parts = streams[key][-1]['id'].split(b"-")
            if int(comp[0]) < int(last_id_parts[0]) or (int(comp[0]) == int(last_id_parts[0]) and int(comp[1]) <= int(last_id_parts[1])):
                conn.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                return

        fields = {}
        for i in range(8, len(command_parts) -1, 4):
            fields[command_parts[i]] = command_parts[i+2]
            
        if key not in streams:
            streams[key] = []
        
        entry = {'id': value, 'fields': fields}
        streams[key].append(entry)
        conn.sendall(string(value))
        
        # Unblock clients waiting for this key
        clients_to_remove = []
        for client_conn, block_info in list(blocking_clients.items()):
            _, listening_keys, listening_ids = block_info
            if key in listening_keys:
                send_xread_response(client_conn, listening_keys, listening_ids)
                clients_to_remove.append(client_conn)
        
        for client_conn in clients_to_remove:
            if client_conn in blocking_clients:
                del blocking_clients[client_conn]

    elif command == b"XRANGE":
        key = command_parts[4]
        start = command_parts[6] if command_parts[6] != b"-" else b"0-0"
        end = command_parts[8] if command_parts[8] != b"+" else b"9999999999999-9999999999999"

        if b"-" not in start: start += b"-0"
        if b"-" not in end: end += b"-9999999999999"

        start_ts, start_seq = map(int, start.split(b'-'))
        end_ts, end_seq = map(int, end.split(b'-'))
        
        entries = []
        if key in streams:
            for entry in streams[key]:
                entry_ts, entry_seq = map(int, entry['id'].split(b'-'))
                if (start_ts < entry_ts < end_ts) or \
                   (start_ts == entry_ts and start_seq <= entry_seq) and (entry_ts < end_ts) or \
                   (end_ts == entry_ts and entry_seq <= end_seq) and (entry_ts > start_ts) or \
                   (start_ts == entry_ts == end_ts and start_seq <= entry_seq <= end_seq):
                    entries.append(entry)
        
        result = b"*" + str(len(entries)).encode() + b"\r\n"
        for entry in entries:
            result += b"*2\r\n" + string(entry['id'])
            fields_resp = b""
            count = 0
            for fkey, fval in entry['fields'].items():
                fields_resp += string(fkey) + string(fval)
                count += 2
            result += b"*" + str(count).encode() + b"\r\n" + fields_resp
        conn.sendall(result)

    elif command == b"XREAD":
        try:
            streams_keyword_index = [p.upper() for p in command_parts].index(b"STREAMS")
        except ValueError:
            conn.sendall(b"-ERR syntax error\r\n")
            return

        is_blocking = b"BLOCK" in [p.upper() for p in command_parts[:streams_keyword_index]]
        
        num_streams = (len(command_parts) - streams_keyword_index - 2) // 4
        keys_start_index = streams_keyword_index + 2
        
        stream_keys = [command_parts[keys_start_index + i*4] for i in range(num_streams)]
        stream_ids = [command_parts[keys_start_index + num_streams*2 + i*2] for i in range(num_streams)]

        # Check for immediate data
        has_data = send_xread_response(None, stream_keys, stream_ids)
        if has_data:
            send_xread_response(conn, stream_keys, stream_ids)
        elif is_blocking:
            block_idx = [p.upper() for p in command_parts].index(b"BLOCK")
            timeout_ms = int(command_parts[block_idx + 2])
            if timeout_ms == 0:
                expire_time = float('inf')
            else:
                expire_time = time.time() + (timeout_ms / 1000.0)
            blocking_clients[conn] = (expire_time, stream_keys, stream_ids)
        else: # Non-blocking and no data
            conn.sendall(b"*0\r\n")
            
    elif command == b"TYPE":
        key = command_parts[4]
        if key in streams:
            conn.sendall(b'+stream\r\n')
        elif key in dictionary:
            conn.sendall(b'+string\r\n')
        else:
            conn.sendall(b"+none\r\n")

    elif command == b"GET":
        key = command_parts[4]
        if key in dictionary and (temp3 is None or time.time() < temp3):
             conn.sendall(string(dictionary[key]))
        else:
            if key in dictionary:
                del dictionary[key]
            conn.sendall(b"$-1\r\n")

    elif command == b"ECHO":
        message = parsing(data)
        if message:
            conn.sendall(string(message))
    else:
        conn.sendall(b"-ERR unknown command\r\n")

def send_xread_response(conn, stream_keys, stream_ids):
    all_matches = []
    has_data = False
    for key, start_id_str in zip(stream_keys, stream_ids):
        if key not in streams:
            continue
        
        if start_id_str == b"$":
             start_id_str = streams[key][-1]['id']

        start_ts, start_seq = map(int, start_id_str.split(b'-'))
        
        current_matches = []
        for entry in streams[key]:
            entry_ts, entry_seq = map(int, entry['id'].split(b'-'))
            if entry_ts > start_ts or (entry_ts == start_ts and entry_seq > start_seq):
                current_matches.append(entry)
                has_data = True
        
        if current_matches:
            stream_result = string(key)
            
            entries_resp = b""
            for entry in current_matches:
                fields_resp = b""
                count = 0
                for fkey, fval in entry['fields'].items():
                    fields_resp += string(fkey) + string(fval)
                    count += 2
                entries_resp += b"*2\r\n" + string(entry['id']) + b"*" + str(count).encode() + b"\r\n" + fields_resp
                
            stream_result += b"*" + str(len(current_matches)).encode() + b"\r\n" + entries_resp
            all_matches.append(b"*2\r\n" + stream_result)
    
    if conn and has_data:
        response = b"*" + str(len(all_matches)).encode() + b"\r\n" + b"".join(all_matches)
        conn.sendall(response)
    
    return has_data

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    sel.register(server_socket, selectors.EVENT_READ, accept)
    
    while True:
        events = sel.select(timeout=0.05) # Short timeout to check for blocking clients
        
        for key, _ in events:
            callback = key.data
            callback(key.fileobj)

        # Handle timed out blocking clients
        now = time.time()
        clients_to_remove = []
        for client_conn, (expire_time, _, _) in list(blocking_clients.items()):
            if now >= expire_time:
                try:
                    client_conn.sendall(b"$-1\r\n") # Send null on timeout
                except:
                    pass # Client may have disconnected
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
