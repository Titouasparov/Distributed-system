import socket

BUF_SIZE = 1024
PORT = 50000  # Change this if using university servers
VERBOSE = True  # Controls printing sent and received messages to the terminal, change to False to disable

# Create socket
sock = socket.socket()
sock.settimeout(2)
sock.connect(("localhost", PORT))

# Function to receive message from socket
def receive() -> str:
    data = b''
    while True:
        try:
            part = sock.recv(BUF_SIZE)
        except (TimeoutError, socket.timeout):
            break
        data += part
        if len(part) < BUF_SIZE:  # Check if reached end of message
            break
    message = data.decode().strip()
    if VERBOSE:
        print("Received:", message)
    return message

# Function to send message through socket
def send(message: str):
    if VERBOSE:
        print("Sent:", message)
    sock.sendall(bytes(f"{message}\n", encoding="utf-8"))

def handshake():
    send("HELO")
    receive() 
    send("AUTH titouan")
    receive()

def atl_scheduling():
    while True:
        send("REDY")
        response = receive()

        if response == "NONE":
            break 

        if response.startswith("JOBN"):
            job = build_job(response)
            send("OK")
            receive()

            server = get_most_cores_server()
            schedule(job, server)
            receive() 

        else:
            continue

    send("QUIT")
    receive()


def get_most_cores_server():
    max_cores = -1
    index_server=-1
    send("GETS All")
    data = receive()
    send("OK")
    servers=receive().split('\n')
    send("OK")
    receive()
    servers = [s.strip() for s in servers]
    for i in range(len(servers)):
        cores = int(servers[i].split(' ')[4])
        if cores>max_cores:
            max_cores = cores
            index_server=i
    return build_server(servers[index_server])

def build_server(server_str):
    keys = ["type", "id", "state", "curStartTime", "core", "memory", "disk", "wJobs", "rJobs"]
    parts = server_str.strip().split()
    return {key: val for key, val in zip(keys, parts)}

def build_job(job_str):
    keys = ["id","submit_time", "cores", "memory", "disk", "est_runtime"]
    parts = job_str.strip().split()[1:]
    return {key: int(val) for key, val in zip(keys, parts)}


def schedule(job, server):
    send("SCHD %s %s %s" % (job['id'], server['type'], server['id']))

handshake()
atl_scheduling()