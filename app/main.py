import socket
import asyncio

# PING = "*1\r\n$4\r\nping\r\n"
PONG = "+PONG\r\n"

async def handle_client(client: socket.socket):
    loop = asyncio.get_event_loop()
    while req := await loop.sock_recv(client, 1024):
        print("receibed request", req, client)
        await loop.sock_sendall(client, PONG.encode())

async def main():
    print("Starting server...")
    server = socket.create_server(("localhost", 6379), reuse_port=True)
    server.setblocking(False)
    server.listen()
    print("Listening on port 6379")
    loop = asyncio.get_event_loop()
    while True:
        client, _ = await loop.sock_accept(server)
        loop.create_task(handle_client(client))


if __name__ == "__main__":
    asyncio.run(main())
