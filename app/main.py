import socket
import asyncio

# PING = "*1\r\n$4\r\nping\r\n"
RESPONSE = "+PONG\r\n"

async def pong(client_socket: socket.socket, loop: asyncio.AbstractEventLoop):
    while data := await loop.sock_recv(client_socket, 1024):
        await loop.sock_sendall(client_socket, RESPONSE.encode())


async def listen(server_socket: socket.socket, loop: asyncio.AbstractEventLoop):
    while True:
        client_socket, addr = await loop.sock_accept(server_socket)
        client_socket.setblocking(False)
        asyncio.create_task(pong(client_socket, loop))


async def main():
    print("Starting server...")
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    loop = asyncio.get_event_loop()
    await listen(server_socket, loop)


if __name__ == "__main__":
    asyncio.run(main())
