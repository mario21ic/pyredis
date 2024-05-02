import socket

RESPONSE = "+PONG\r\n"

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept() # wait for client
    data = conn.recv(1024).decode() # read data from client
    print(data)
    conn.sendall(f"{RESPONSE}".encode()) # send data to client
    print("Response sent")


if __name__ == "__main__":
    main()
