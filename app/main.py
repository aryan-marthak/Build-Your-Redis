import socket
import selectors

sel = selectors.DefaultSelector()

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
    data = conn.recv(1024)
    if not data:
        sel.unregister(conn)
        conn.close()
        return
    
    if b"SET" in data.upper():
        conn.sendall(b"+OK\r\n")
        split = data.split(b"\r\n")
        global temp1, temp2
        temp1 = split[4]
        temp2 = split[6]

    elif b"PING" in data.upper():
        conn.sendall(b"+PONG\r\n")
    elif b"GET" in data.upper():
        split = data.split(b"\r\n")
        if temp1 == split[2]:
            res = string(temp2)
            conn.sendall(res)
        temp = parsing(data)
        if temp is not None:
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
