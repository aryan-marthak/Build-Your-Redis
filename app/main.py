import socket

def parse_resp(data):
    """Parse RESP array and return list of strings like ['ECHO', 'hey']"""
    parts = data.split(b"\r\n")
    count = int(parts[0][1:])  # number of elements in array
    result = []
    i = 1
    while len(result) < count:
        if parts[i].startswith(b"$"):
            length = int(parts[i][1:])
            result.append(parts[i + 1])
            i += 2
        else:
            i += 1
    return result

def main():
    server = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server.accept()
        data = conn.recv(1024)
        if not data:
            conn.close()
            continue

        try:
            cmd_parts = parse_resp(data)
            command = cmd_parts[0].decode().upper()

            if command == "PING":
                conn.sendall(b"+PONG\r\n")
            elif command == "ECHO" and len(cmd_parts) == 2:
                msg = cmd_parts[1]
                resp = b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
                conn.sendall(resp)
            else:
                conn.sendall(b"-ERR unknown command\r\n")
        except Exception as e:
            conn.sendall(b"-ERR parsing failed\r\n")

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
