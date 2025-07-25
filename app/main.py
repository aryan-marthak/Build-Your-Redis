import socket  # noqa: F401
import threading

def handle_client(connection):
    while True:
        data = connection.recv(1024)
        if b"PING" in data.upper():
            connection.sendall(b"+PONG\r\n")
    

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept() 
        thread = threading.Thread(target=handle_client, args=(connection,))


if __name__ == "__main__":
    main()
