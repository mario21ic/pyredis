import socket

PING = "*1\r\n$4\r\nping\r\n"
RESPONSE = "+PONG\r\n"

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept() # wait for client
    with conn:
        while True:
            data = conn.recv(1024).decode() # read data from client
            if not data:
                break
            print(data)
            if data == PING:
                conn.sendall(f"{RESPONSE}".encode()) # send data to client
                print("Response sent")


if __name__ == "__main__":
    main()
