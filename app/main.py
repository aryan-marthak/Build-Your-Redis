import socket

def parse_command(data):
    parts = data.split(b"\r\n")
    if parts[0].startswith(b"*") and parts[2].startswith(b"$"):
        command = parts[2].decode().upper()
        if command == "PING":
            return b"+PONG\r\n"
        elif command == "ECHO" and len(parts) > 4:
            message = parts[4]
            return b"$" + str(len(message)).encode() + b"\r\n" + message + b"\r\n"
    return None

def main():
    server = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server.accept()
        data = conn.recv(1024)
        if not data:
            conn.close()
            continue

        response = parse_command(data)
        if response:
            conn.sendall(response)
        conn.close()

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
