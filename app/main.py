import socket
import selectors

sel = selectors.DefaultSelector()

def accept(sock):
    conn, _ = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

def read(conn):
    try:
        data = conn.recv(1024)
        if not data:
            sel.unregister(conn)
            conn.close()
            return
        if b'PING' in data:
            conn.send(b'+PONG\r\n')
        elif b'ECHO' in data:
            # RESP: *2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n
            parts = data.split(b'\r\n')
            if len(parts) >= 6:
                msg = parts[5]
                resp = b'$' + str(len(msg)).encode() + b'\r\n' + msg + b'\r\n'
                conn.send(resp)
    except ConnectionResetError:
        sel.unregister(conn)
        conn.close()

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 6379))
    server_socket.listen()
    server_socket.setblocking(False)
    sel.register(server_socket, selectors.EVENT_READ, accept)

    while True:
        for key, _ in sel.select():
            callback = key.data
            callback(key.fileobj)

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
