import socket
import asyncio
import threading
import argparse
import sys

# PING = "*1\r\n$4\r\nping\r\n"
PONG = "+PONG\r\n"
OK = "+OK\r\n"
NULL = "$-1\r\n"
KEY_VALUES = {}
ROLE = "master"

# https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
def create_bulk_response(msg):
    bulk_resp = "$" + str(len(msg)) + "\r\n" + msg + "\r\n"
    print("bulk_resp", bulk_resp)
    return bulk_resp

async def count_down(key: str, px: int = None):
    def delete_key():
        print("delete_key - time expired:", key)
        if key in KEY_VALUES:
            del KEY_VALUES[key]

    timer = threading.Timer(px / 1000, delete_key)
    timer.start()


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
                print("msg: ", req.decode().split("\r\n"))
                key = req.decode().split("\r\n")[4]
                value = req.decode().split("\r\n")[6]
                print(f"key: {key} - value: {value}")
                KEY_VALUES[key] = value
                if len(req.decode().split("\r\n")) > 8:
                    if req.decode().split("\r\n")[8].lower() == "px":
                        # param = req.decode().split("\r\n")[8]
                        expiry = int(req.decode().split("\r\n")[10])
                        print("expiry:", expiry)
                        asyncio.create_task(count_down(key, expiry))
                await loop.sock_sendall(client, OK.encode())
            case "get":
                data = KEY_VALUES.get(req.decode().split("\r\n")[4], "")
                print("data:", data)
                if data != "":
                    data = create_bulk_response(data)
                else:
                    # print("data not found - sending NULL response")
                    data = NULL
                await loop.sock_sendall(client, data.encode())
            case "info":
                param = req.decode().split("\r\n")[4]
                # global ROLE
                if param == "replication":
                    await loop.sock_sendall(client, create_bulk_response("# Replication\r\nrole:" + ROLE).encode())


async def main():
    print("Starting server...")
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", type=int, default=6379, metavar="PORT")
    parser.add_argument("--replicaof", nargs=2, default="", metavar=("MASTER_HOST", "MASTER_PORT"))
    args = parser.parse_args(sys.argv[1:])

    port = 6379
    if args.port:
        # print(f"port turned on: {args.port}")
        port = args.port
    
    if len(args.replicaof) > 0:
        print(f"replicaof turned on: {args.replicaof}")
        global ROLE
        ROLE = "slave"

    server = socket.create_server(("localhost", port), reuse_port=False)
    server.setblocking(False)
    server.listen()
    print(f"Listening on port {port}")
    loop = asyncio.get_event_loop()
    while True:
        client, _ = await loop.sock_accept(server)
        loop.create_task(handle_client(client))


if __name__ == "__main__":
    asyncio.run(main())
