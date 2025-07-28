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
        
        fields = {}
        for i in range(8, len(split) - 1, 2):
            if i + 1 < len(split) and split[i] and split[i + 1]:
                fields[split[i]] = split[i + 1]
        
        if key not in streams:
            streams[key] = []
        
        entry = {'id': value, 'fields': fields}
        
        streams[key].append(entry)
        
        conn.sendall(string(value))
        
    
    elif b"TYPE" in data.upper():
        split = data.split(b"\r\n")
        if key in streams:
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
