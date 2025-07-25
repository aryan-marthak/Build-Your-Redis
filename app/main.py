import socket  # noqa: F401


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept() 
    while True:
        data = connection.recv(1024)
        if b"PING" in data.upper():
            connection.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
