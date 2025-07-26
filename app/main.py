import socket
import selectors

sel = selectors.DefaultSelector()

def accept(sock):
    conn, addr = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

def read(conn):
    data = conn.recv(1024)
    if not data:
        sel.unregister(conn)
        conn.close()
        return

    try:
        parts = data.split(b'\r\n')
        if parts[0].startswith(b'*') and parts[2].upper() == b'ECHO':
            msg = parts[4]
            resp = f"${len(msg)}\r\n{msg.decode()}\r\n".encode()
            conn.send(resp)
        elif parts[0].startswith(b'*') and parts[2].upper() == b'PING':
            conn.send(b"+PONG\r\n")
        else:
            conn.send(b"-ERR unknown command\r\n")
    except:
        conn.send(b"-ERR parsing failed\r\n")

sock = socket.socket()
sock.bind(('localhost', 6379))
sock.listen()
sock.setblocking(False)
sel.register(sock, selectors.EVENT_READ, accept)

while True:
    for key, _ in sel.select():
        callback = key.data
        callback(key.fileobj)



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
