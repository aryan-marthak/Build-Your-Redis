import socket

def parsing(data, conn):
    if b"PING" in data.upper():
        conn.sendall(b"+PONG\r\n")
    split = data.split(b"\r\n")
    if split[0].startswith(b"*") and split[2].startswith(b"$"):
        echo = split[2].decode().upper()
        if echo == "ECHO":
            return split[4]
    return None

def string(words):
    return b"$" + str(len(words)).encode() + b"\r\n" + words + b"\r\n"


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()
        data = conn.recv(1024)
        temp = parsing(data, conn)
        res = string(temp)
        conn.sendall(res)

if __name__ == "__main__":
    main()

# import socket  # noqa: F401
# import selectors

# sel = selectors.DefaultSelector()

# def accept(sock):
#     conn, _ = sock.accept()
#     conn.setblocking(False)
#     sel.register(conn, selectors.EVENT_READ, read)

# def read(conn):
#     data = conn.recv(1024)
#     if b"PING" in data.upper():
#         conn.sendall(b"+PONG\r\n")

# def main():
#     server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
#     server_socket.setblocking(False)
#     sel.register(server_socket, selectors.EVENT_READ, accept)
    
#     while True:
#         events = sel.select()
#         for key, _ in events:
#             callback = key.data
#             callback(key.fileobj)

# if __name__ == "__main__":
#     main()

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
