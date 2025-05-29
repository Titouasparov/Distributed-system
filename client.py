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

            servers = get_capable_servers(job)
            if (len(servers)>0):
                server = get_most_cores_server(servers)
                schedule(job, server)
                receive()

        else:
            continue

    send("QUIT")
    receive()

def get_capable_servers(job):
    send(f'GETS Capable {job["cores"]} {job["memory"]} {job["disk"]}')
    _ = receive()
    send('OK')
    servers = receive().split('\n')
    servers = [s.strip() for s in servers]
    send('OK')
    receive()
    servers = list(map(build_server,servers))
    return servers


def get_most_cores_server(servers):
    max_cores = -1
    index_server=-1
    for i in range(len(servers)):
        cores = servers[i]['cores']
        if cores>max_cores:
            max_cores = cores
            index_server=i
    return servers[index_server]

def build_server(server_str):
    keys = ["type", "id", "state", "curStartTime", "cores", "memory", "disk", "wJobs", "rJobs"]
    parts = server_str.strip().split()
    result = {}
    for key, val in zip(keys, parts):
        if key in ["id", "curStartTime", "cores", "memory", "disk", "wJobs", "rJobs"]:
            result[key] = int(val)
        else:
            result[key] = val
    return result

def build_job(job_str):
    keys = ["id","submit_time", "cores", "memory", "disk", "est_runtime"]
    parts = job_str.strip().split()[1:]
    return {key: int(val) for key, val in zip(keys, parts)}


def schedule(job, server):
    send("SCHD %s %s %s" % (job['id'], server['type'], server['id']))

handshake()
atl_scheduling()