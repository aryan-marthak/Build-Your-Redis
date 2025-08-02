import socket
import selectors
import time

sel = selectors.DefaultSelector()
dictionary, temp1, temp2, temp3 = {}, b"", b"" , None
streams = {}

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
    
def read(conn):
    global dictionary, temp3, temp1, temp2, streams
    data = conn.recv(1024)
    if not data:
        sel.unregister(conn)
        conn.close()
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
                        
                else:
                    if divide[0] == b"0" and last == -1:
                        New = 1
                    else:
                        New = 0
                value = divide[0] + b"-" + str(New).encode()
                
        
        comp = value.split(b"-")
        if comp[0] == b"0" and comp[1] == b"0":
            conn.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
            return        

        
        if key in streams:
            last = streams[key][-1]
            temp = last['id'].split(b"-")
            if comp[0] < temp[0] or (comp[0] == temp[0] and comp[1] <= temp[1]):
                conn.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                return

        fields = {}
        for i in range(8, len(split), 4):
            if i + 2 < len(split):
                fname = split[i]
                fval = split[i + 2]
                if fname and fval:
                    fields[fname] = fval
                            
        if key not in streams:
            streams[key] = []
        
        entry = {'id': value, 'fields': fields}
        
        streams[key].append(entry)
        
        conn.sendall(string(value))
        
    elif b"XRANGE" in data.upper():
        xrange_split = data.split(b"\r\n")
        xrange_var = xrange_split[4]
        xrange_start = xrange_split[6]
        xrange_end = xrange_split[8]
        
        if b"-" not in xrange_start:
            xrange_start = xrange_start + b"-0"
        if xrange_end == b"+":
            xrange_end = b"9999999999-9999999999"
        elif b"-" not in xrange_end:
            xrange_end = xrange_end + b"-9999999999"
            
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
        
        block_time = 0
        
        if b"BLOCK" in xread_split:
            block_time = int(time.time()) * 1000 + int(xread_split[6])
            
        
        stream_start = xread_split.index(b"streams") + 1
        total = (len(xread_split) - stream_start) // 2
        stream_keys = xread_split[stream_start : stream_start + total]
        stream_ids = xread_split[stream_start + total : stream_start + 2*total]
        
        if int(time.time()) * 1000 > block_time:
            conn.sendall(b"$-1\r\n")
        else:
            matches = []

            for k, i in zip(stream_keys, stream_ids):
                if b"-" not in i:
                    i += b"-0"
    
                matched = []
                if k in streams:
                    for enter in streams[k]:
                        if enter["id"] > i:
                            matched.append(enter)
    
                if matched:
                    stream_result = b"*2\r\n"
                    stream_result += string(k)
                    stream_result += b"*" + str(len(matched)).encode() + b"\r\n"
    
                    for entry in matched:
                        stream_result += b"*2\r\n"
                        stream_result += string(entry['id'])
    
                        fields = entry["fields"]
                        stream_result += b"*" + str(len(fields) * 2).encode() + b"\r\n"
                        for fk, fv in fields.items():
                            stream_result += string(fk)
                            stream_result += string(fv)
    
                    matches.append(stream_result)
    
            result = b"*" + str(len(matches)).encode() + b"\r\n" + b"".join(matches)
            conn.sendall(result)
        
    
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


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    sel.register(server_socket, selectors.EVENT_READ, accept)
    while True:
        events = sel.select()
        for key, _ in events:
            callback = key.data
            callback(key.fileobj)

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
