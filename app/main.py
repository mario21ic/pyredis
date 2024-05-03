import socket
import asyncio


# PING = "*1\r\n$4\r\nping\r\n"
PONG = "+PONG\r\n"
OK = "+OK\r\n"
KEY_VALUES = {}

# https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
def create_bulk_response(msg):
    bulk_resp = "$" + str(len(msg)) + "\r\n" + msg + "\r\n"
    print("bulk_resp", bulk_resp)
    return bulk_resp


async def handle_client(client: socket.socket):
    loop = asyncio.get_event_loop()
    while req := await loop.sock_recv(client, 1024):
        print(f"received request: {req} - client: {client}")
        cmd = req.decode().split("\r\n")[2].lower()
        match cmd:
            case "ping":
                await loop.sock_sendall(client, PONG.encode())
            case "echo":
                msg = req.decode().split("\r\n")[4]
                await loop.sock_sendall(client, create_bulk_response(msg).encode())
            case "set":
                key = req.decode().split("\r\n")[4]
                value = req.decode().split("\r\n")[6]
                KEY_VALUES[key] = value
                print(f"key: {key} - value: {value}")
                await loop.sock_sendall(client, OK.encode())
            case "get":
                await loop.sock_sendall(client, create_bulk_response(KEY_VALUES.get(req.decode().split("\r\n")[4], "")).encode())


async def main():
    print("Starting server...")
    server = socket.create_server(("localhost", 6379), reuse_port=False)
    server.setblocking(False)
    server.listen()
    print("Listening on port 6379")
    loop = asyncio.get_event_loop()
    while True:
        client, _ = await loop.sock_accept(server)
        loop.create_task(handle_client(client))


if __name__ == "__main__":
    asyncio.run(main())
