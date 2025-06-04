import socket

BUF_SIZE = 1024
PORT = 50000  # Change this if using university servers
VERBOSE = False  # Controls printing sent and received messages to the terminal, change to False to disable

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

def scheduling():
    #first redy
    send('REDY')
    while True:
        schedule_from_queue() #We prioritize jobs waiting in the queue for reducing the turnaround time (tat)
        send("REDY")
        response = receive()

        if response == "NONE": #no job anymore
            break 

        if (response.startswith("JOBN")):
            send("OK")
            job = build_job(receive())
            servers = get_available_servers(job)
            if (servers):
                best_server = min(servers, key=lambda s: score_server(s,job)) #computing the best server according to the scoring function
                schedule(job,best_server)
            else: #no server available
                add_queu() #we enqueue the job
        else:
            continue

    send("QUIT")
    receive()

#try to schedule all the jobs in the queue, shortest jobs goe first
def schedule_from_queue():
    while True:
        queue = get_queue()
        if queue:
            min_index, min_job = min(enumerate(queue), key=lambda x: x[1]['est_runtime']) #shortes job in terms of estimated execution time
            servers = get_available_servers(min_job)
            if servers: #there are servers available
                best = min(servers, key=lambda s: score_server(s, min_job)) #get the best one according to our metric
                unqueue(min_index)
                schedule(min_job, best)
            else:
                break
        else:
            break

#the score prioritize tight scheduling (i.e selecting the server that fits the job requirements as closely as possible)
def score_server(server, job):
    # Server resources
    cores_avail = server['cores']
    mem_avail = server['memory']
    disk_avail = server['disk']

    # Job requirements
    job_cores = job['cores']
    job_memory = job['memory']
    job_disk = job['disk']

    # Reject if server lacks capacity
    if cores_avail < job_cores or mem_avail < job_memory or disk_avail < job_disk:
        return float('inf')

    # Tight packing score (how closely the job fits the server)
    cpu_util = job_cores / cores_avail
    mem_util = job_memory / mem_avail
    disk_util = job_disk / disk_avail
    tightness_score = 1 - ((3*cpu_util + mem_util + disk_util) / 5) 

    capacity_score = (cores_avail + mem_avail + disk_avail) / 3

    states = {'idle':-0.3,
              'active': -0.1,
              'inactive':1,
              }

    # Final score: lower is better
    score = (min(0.001,tightness_score + states.get(server['state'],0)))*capacity_score

    return score


#return the list of available servers for a given job
def get_available_servers(job):
    send(f'GETS Avail {job["cores"]} {job["memory"]} {job["disk"]}')
    header = receive()

    if header.startswith("DATA 0"):
        send("OK")
        receive()
        return []  # Aucun serveur dispo

    send("OK")
    servers = receive().split('\n')
    servers = [s.strip() for s in servers if s.strip()]
    send("OK")
    receive()
    
    return list(map(build_server, servers))

#add a job the the general queue
def add_queu():
    send('ENQJ GQ')
    receive()

#unqueue the job at index id
def unqueue(id):
    send(f'DEQJ GQ {id}')
    receive()

#return the list of the jobs in the generak queue
def get_queue():
    send(f'LSTQ GQ *')
    if int(receive().strip().split()[1]) == 0:
        send('OK')
        receive()
        return []
    send('OK')
    jobs = receive().split('\n')
    send('OK')
    receive()
    return list(map(build_job_from_queue,jobs))

#build a dictionary representing the server from the server get output
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

#same as build_server for jobs
def build_job(job_str):
    keys = ["id","submit_time", "cores", "memory", "disk", "est_runtime"]
    parts = job_str.strip().split()[1:]
    return {key: int(val) for key, val in zip(keys, parts)}

#same as other build functions for a job from a queue
def build_job_from_queue(queue_str):
    keys = ["id","submit_time", "cores", "memory", "disk", "est_runtime"]
    parts = queue_str.strip().split()
    vals = [
        parts[0],  # jobID -> id
        parts[2],  # submit_time
        parts[5],  # cores
        parts[6],  # memory
        parts[7],  # disk
        parts[4],  # est_runtime
    ]
    return {key: int(val) for key, val in zip(keys, vals)}

#schedules one job on the server. The job has to be a dictionary built with build_job functions
def schedule(job, server):
    send("SCHD %s %s %s" % (job['id'], server['type'], server['id']))
    receive()

#start the scheduling
handshake()
scheduling()