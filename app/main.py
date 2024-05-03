import socket
import asyncio
import threading
import argparse
import sys

PING = "*1\r\n$4\r\nping\r\n"
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

"""
# REPLCONF listening-port <PORT>
*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n
# REPLCONF capa psync2
*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n
https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
"""
def bulk_array(text):
    message = text.split(" ")
    bulk = "*" + str(len(message)) + "\r\n"
    for msg in message:
        bulk += "$" + str(len(msg)) + "\r\n" + msg + "\r\n"
    print("bulk", bulk)
    return bulk

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
        print(f"cmd: {cmd}")
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
                    data = NULL
                await loop.sock_sendall(client, data.encode())
            case "info":
                param = req.decode().split("\r\n")[4]
                if param == "replication":
                    await loop.sock_sendall(client, create_bulk_response(f"# Replication\r\nrole:{ROLE}\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n").encode())
            case "replconf":
                await loop.sock_sendall(client, OK.encode())
            case _:
                await loop.sock_sendall(client, create_bulk_response("Command not found").encode())


async def main():
    print("Starting server...")
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", type=int, default=6379, metavar="PORT")
    parser.add_argument("--replicaof", nargs=2, default="", metavar=("MASTER_HOST", "MASTER_PORT"))
    args = parser.parse_args(sys.argv[1:])

    host_port = 6379
    if args.port:
        # print(f"port turned on: {args.port}")
        host_port = args.port
    
    # Mode Replica
    if len(args.replicaof) > 0:
        global ROLE
        ROLE = "slave"
        master_host, master_port = args.replicaof
        print(f"ReplicaOf Host: {master_host} - port: {master_port}")
        # Send Ping to Master
        master_sock = socket.create_connection((master_host, int(master_port)))
        master_sock.sendall(PING.encode())
        
        # Recibir respuesta del servidor
        response = master_sock.recv(1024)  # 1024 es el tamaño del buffer de recepción
        print("response", response.decode())
        if response.decode() == PONG:
            print("Master is alive")
            
            replconf1 = f"REPLCONF listening-port {host_port}"
            master_sock.sendall(bulk_array(replconf1).encode())
            response = master_sock.recv(1024)
            print("response", response.decode())

            replconf2 = "REPLCONF capa psync2"
            master_sock.sendall(bulk_array(replconf2).encode())
            response = master_sock.recv(1024)
            print("response", response.decode())
        
        # Close connection
        master_sock.close()

    server = socket.create_server(("localhost", host_port), reuse_port=False)
    server.setblocking(False)
    server.listen()
    print(f"Listening on port {host_port}")
    loop = asyncio.get_event_loop()
    while True:
        client, _ = await loop.sock_accept(server)
        loop.create_task(handle_client(client))


if __name__ == "__main__":
    asyncio.run(main())
