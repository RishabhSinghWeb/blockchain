import socket, json, time, select, sys, uuid, hashlib, random

WEBSITE_PORT = 8998
FLOOD_TIMER = 28 # not 30 because 30*2=60 by that time peers already assume us offline
PEER_OFFLINE_TIME = 60
CONCENSUS_INTERVAL = 600
SYNC_INTERVAL = 60
DIFFICULTY = 5  # difficulty 5 takes around 2 seconds, 6 takes 25 sec, 7 takes 2.5 min, 8 takes 30 min
GET_BLOCK_WAIT = 2

now = int(time.time())
consensus_timer = now - (CONCENSUS_INTERVAL - 5) # 5 sec delayed startup
last_sync_time = now - SYNC_INTERVAL + 5 # 5 sec delayed startup waiting for peers to connect
last_block_wait_time = 0
last_flood_time = 0
nonce = 0
stats = []
mining_queue = []

# starting sockets
host = '' # '0.0.0.0'
try:
    port = int(sys.argv[1])
except:
    port = 8999

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind((host, port))
sock2 = socket.socket()
inputs = [sock]
print('Listening on udp %s:%s' % (host, port))
try:
    sock2.bind((host, WEBSITE_PORT))
    sock2.listen(5)
    print('Listening on tcp %s:%s' % (host, WEBSITE_PORT))
    inputs.append(sock2)
except:
    print(f"WEBSITE_PORT:{WEBSITE_PORT} is not available")
    

# get external ip address
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    s.connect(("8.8.8.8", 80))
    host = s.getsockname()[0]
    s.close()
except:
    host = socket.gethostbyname((socket.gethostname()))

peers = [
            {'id':None, 'port':8888, 'name':None, 'time':now, 'host':'localhost'},
            {'id':None, 'port':8000, 'name':None, 'time':now, 'host':host},
            {'id':None, 'port':8999, 'name':None, 'time':now, 'host':host},
            {'id':None, 'port':8999, 'name':None, 'time':now, 'host':'murr.ga'}
        ]

class Block():

    def __init__(self, block):
        super(Block, self).__init__()
        messages = block['messages']
        if type(messages) == "<class 'str'>":
            messages = [messages]
        self.messages = messages
        self.nonce = block['nonce']
        self.minedBy = block['minedBy']
        self.timestamp = block['timestamp']
        self.hash = block['hash']
        self.height = block['height']

    def __str__(self):
        return f"{{Height:{self.height},Messages:{self.messages},Hash:'{self.hash}'}}"

    def json(self):
        return json.dumps({
            'hash':self.hash,
            'height': self.height,
            'messages': self.messages,
            'minedBy': self.minedBy,
            'nonce': self.nonce,
            'timestamp': self.timestamp,
            'type': 'GET_BLOCK_REPLY'
            })

    def verify(self, blockchain):
        flag = True
        if (len(self.messages) < 1 or len(self.messages) > 10 or len(self.nonce) > 40): # check length
            flag = False
        for message in self.messages:
            if len(message) >= 20: # check length of each message
                flag = False
        m = hashlib.sha256()
        if self.height > 0:
            try:
                previous_block = blockchain.Blockchain[self.height-1]
                m.update(previous_block.hash.encode())
            except:
                flag = False
        m.update(self.minedBy.encode())
        for message in self.messages:
            m.update(message.encode())
        m.update(self.timestamp.to_bytes(8, 'big'))
        m.update(self.nonce.encode())
        digest = m.hexdigest()
        if len(digest) - len(digest.rstrip('0')) < DIFFICULTY: # check difficulty
           flag = False
        return flag


class Blockchain():

    def __init__(self):
        super(Blockchain, self).__init__()
        self.Blockchain = []

    def hash(self):
        if self.Blockchain:
            return self.Blockchain[-1].hash
        else:
            return None

    def height(self):
        return len(self.Blockchain)-1

    def __str__(self):
        return f"{self.height()}_{self.hash()}"

    def append(self, block):
        if block.height == 0 and block.verify():
            self.Blockchain.append(block)
        elif block.verify(blockchain):   # only add if verified i.e. whole chain is always valid
            self.Blockchain.append(block)
            return True
        else:
            return False

    # This blockchain will always be valid, 100% of the time
    # so verifing chain is unnecessary
    def verify_chain(self, blockchain):
        flag = True
        for block in self.Blockchain:
            if not block.verify(blockchain):
                flag = False
        return flag

    def verify_target(self, target):
        flag = True
        if not self.verify_chain(self):
            flag = False
        if target['height'] >= 0 and target['hash']:
            if self.height() != target['height'] or self.hash() != target['hash']:
                flag = False
        return flag



blockchain = Blockchain()
target = {'height':-1, 'hash':None}
Blockchain_buffer = []
name = f'{str(port)} here!'


# Main Loop
while True:
    now = int(time.time())

    # Clean up peers
    for peer in peers:
        if now - peer['time'] > PEER_OFFLINE_TIME: # remove those who have not sent FLOOD messages
            peers.remove(peer)
        elif peer['host'] == host and peer['port'] == port: # remove self from peer list
            peers.remove(peer)

    # Setting the target by choosing the longest chain (ties break on majority)
    # most_popular_counts = dict(((i['height'],stat['hash']), stats.count({'hash':stat['height'],'hash':stat['hash']})) for stat in stats)
    highest_stats_counts = {}
    highest_stats = [(target['height'], target['hash'])]
    for stat in stats:
        stat_tuple = (stat['height'],stat['hash'])
        if now - stat['time'] > 20: # Clean up old stats
            stats.remove(stat)
        elif stat['hash'] and stat['height'] >= blockchain.height():
            if stat['height'] > highest_stats[0][0]: # Never move to a shorter chain
                highest_stats = [stat_tuple]
            elif stat['height'] == highest_stats[0][0]:
                highest_stats.append(stat_tuple)

    for stat in highest_stats:
        highest_stats_counts[stat] = highest_stats.count(stat)
    # Ties break on most-agreed-upon chain
    target['height'], target['hash'] = sorted(highest_stats_counts, key=highest_stats_counts.get, reverse=True)[0]


    # Checking the pending process by loading the buffer blocks
    if not blockchain.verify_target(target):
        if target['height'] >= blockchain.height() or target['height'] != blockchain.hash():
            for buffer in Blockchain_buffer:
                block = buffer['block']
                if now - buffer['time'] > 30:
                    Blockchain_buffer.remove(buffer)
                elif block.height == blockchain.height():
                    blockchain.append(block)
                    Blockchain_buffer.remove(buffer)

            for buffer in Blockchain_buffer:
                block = buffer['block']
                if block.height == blockchain.height()+1:
                    try:
                        blockchain.append(block)
                    except:
                        print("Invalid block", block)
                    Blockchain_buffer.remove(buffer)
                        
                    
    elif target['hash'] == blockchain.hash():
        Blockchain_buffer = []

    # Flood messages on Regular interval
    if now - last_flood_time > FLOOD_TIMER: 
        for peer in peers: # sent to all peers
            try:
                sock.sendto(json.dumps({
                    'type': 'FLOOD',
                    'host': host,
                    'port': port,
                    'id': str(uuid.uuid4()),
                    'name': name
                    }).encode(), (peer['host'],peer['port']))
            except:
                print("Unable to reach peer: ", peer)
        last_flood_time = now

    # Consensus
    # On joining or get request - 5 second delayed from startup
    if now - consensus_timer > CONCENSUS_INTERVAL:
        for peer in peers:
            try:
                sock.sendto(json.dumps({
                    'type': 'CONSENSUS'
                    }).encode(), (peer['host'],peer['port']))
            except:
                print("Unable to reach peer: ", peer)
        if blockchain.verify_target(target): # Collect blocks because chain is verified target chain
            pass # manage in main loop
        else: # Move to new chain that we chose in consensus
            blockchain = Blockchain()

    # syncing blockchain
    if now - last_sync_time > SYNC_INTERVAL:
        for peer in peers:
            try:
                addr_req = (peer['host'], peer['port'])
                sock.sendto(json.dumps({
                        "type": "STATS"
                    }).encode(), addr_req)
            except:
                print("Unable to reach peer: ", peer)
        last_sync_time = now


    # getting pending blocks 
    if now - last_block_wait_time > GET_BLOCK_WAIT:
        for buffer in Blockchain_buffer:
            block = buffer['block']
            if block.height == blockchain.height()+1:
                break
        else: # if required block is not in buffer after timeout
            target_peers = []
            for stat in stats:
                if stat['height'] == target['height'] and stat['hash'] == target['hash']:
                    target_peers.append(stat)
            if not target_peers:
                target_peers = stats
            for height in range(blockchain.height()+1, target['height']):
                target_peer = random.choice(target_peers) # load balancing to target peers
                sock.sendto(json.dumps({
                    "type": "GET_BLOCK",
                    "height": height
                    }).encode(),target_peer['addr'])
        last_block_wait_time = now

    # mining new messages
    if mining_queue and blockchain.verify_target(target):
        height = blockchain.height()+1
        messages = mining_queue[0]
        m = hashlib.sha256()
        if height > 0:
            previous_block = blockchain.Blockchain[height-1]
            m.update(previous_block.hash.encode())
        m.update(name.encode())
        for message in messages:
            m.update(message.encode())
        m.update(now.to_bytes(8, 'big'))
        iterations = 100000
        for i in range(iterations):
            nonce+=1
            x = m.copy()
            x.update(str(nonce).encode())
            digest = x.hexdigest()
            if len(digest) - len(digest.rstrip('0')) >= DIFFICULTY: # check difficulty
                # mined block, found nonce
                block = {
                        'messages' : messages,
                        'nonce' : str(nonce),
                        'minedBy' : name,
                        'timestamp' : now,
                        'hash' : digest,
                        'height' : height
                    }
                print("Block Mined",Block(block), Block(block).verify(blockchain))
                blockchain.append(Block(block))
                mining_queue.remove(mining_queue[0])
                nonce = 0
                block['type'] = "ANNOUNCE"
                target['height'] = height
                target['hash'] = digest
                for peer in peers:
                    try:
                        addr_req = (peer['host'], peer['port'])
                        sock.sendto(json.dumps(block).encode(), addr_req)
                        sock.sendto(json.dumps({
                                "type": "CONSENSUS"
                            }).encode(), addr_req)
                    except:
                        print("Unable to reach peer: ", peer)
                break
        if nonce > 10**40 - iterations:
            nonce = 0

    # handling socket request
    read_sockets, write_sockets, error_sockets = select.select( inputs, inputs, [], 5)
    for client in read_sockets:

        # browser request
        if client.getsockname()[1] == WEBSITE_PORT:
            if client is sock2:  # New Connection
                clientsock, clientaddr = client.accept()
                inputs.append(clientsock)
            else:  # Existing Connection
                print('Got request on', WEBSITE_PORT, 'from client:', client.getpeername())
                data = client.recv(1024)
                peers_table = "<thead><td>Who<td>Where<td>last_ping</thead>"
                for peer in peers:
                    peers_table += f"""<tr><td>{peer['name']}</td><td>{peer['host']}:{peer['port']}</td>"
                                       <td>{int(now-peer['time'])} seconds ago</td></tr>"""
                chain = "<thead><td>Height<td>Mined By<td>Nonce<td>Messages<td>Hash<td>Difficulty<td>When</thead>"
                for block in blockchain.Blockchain[::-1]:
                    chain += f"<tr><td>{block.height}</td><td>{block.minedBy}</td><td>{block.nonce}</td><td><ul>"
                    for message in block.messages:
                        chain += f"<li>{message}</li>"
                    chain += f"""</ul></td><td>{block.hash}</td><td>{DIFFICULTY}</td>
                                 <td>{block.timestamp}<br>{time.ctime(block.timestamp)}</td></tr>"""

                # send HTTP response to the browser
                client.send(f"""HTTP/1.1 200 OK\nContent-Type: html\n\r\n\r\n
                        <h1>Blockchain Status</h1>
                        hosted on: {host}
                        <h2>Current peers</h2>
                        <table border="1">{peers_table}</table>
                        <h2>Data to mine</h2>
                        <h3>Currently mining</h3>{mining_queue}
                        <h3>Mining Overflow</h3>
                        <h2>The Chain</h2>
                        <table border="1">{chain}</table>
                        <style>table{{border-collapse:collapse}}thead{{text-align:center;font-weight: bold;}}</style>
                    """.encode())
                client.close()
                inputs.remove(client)

        # udp peer request
        else:
            try:
                data, addr = client.recvfrom(5*1024)
            except:
                continue
            print('recv %r - %r\n\n' % addr, data)
            req = json.loads(data.decode())

            if req['type'] == 'FLOOD':
                addr_req = (str(req['host'])), int(req['port'])
                for peer in peers:
                    if peer['host'] == req['host'] and peer['port'] == peer['port']: # old peer
                        peer['time'] = time.time()  # update time for clean up timeout
                        break
                else:  # new peer
                    peers.append({
                        'id': req['id'],
                        'port': int(req['port']),
                        'time': now,
                        'host': str(req['host']),
                        'name': req['name']
                        })

                client.sendto(json.dumps({
                    'type': 'FLOOD_REPLY',
                    'host': host,
                    'port': port,
                    'name': name
                    }).encode(), addr_req) # not replied to sender i.e. addr

            elif req['type'] == 'STATS':
                if blockchain.Blockchain:
                    block_hash = blockchain.Blockchain[-1].hash
                else:
                    block_hash = None
                client.sendto(json.dumps({
                    "type": "STATS_REPLY",
                    'height':blockchain.height(),
                    'hash': block_hash
                    }).encode(),addr)

            elif req['type'] == 'STATS_REPLY':
                for stat in stats:
                    if stat['addr'] == addr and stat['height'] == req['height'] and stat['hash'] == req['hash']:
                        stat[time] = now
                        break
                else:
                    req.pop('type')
                    req['addr'] = addr
                    req['time'] = now
                    stats.append(req)

            elif req['type'] == 'GET_BLOCK':
                try:
                    client.sendto(blockchain.Blockchain[req['height']].json().encode(),addr)
                except:
                    client.sendto(json.dumps({
                            'hash':None,
                            'height': None,
                            'messages': None,
                            'minedBy': None,
                            'nonce': None,
                            'timestamp': None,
                            'type': 'GET_BLOCK_REPLY'
                        }).encode(),addr)

            elif req['type'] == 'GET_BLOCK_REPLY':
                block = Block(req)
                if block.hash:
                    if blockchain.append(block):
                        pass
                    else:
                        Blockchain_buffer.append({'block':block, 'time':now})


            elif req['type'] == 'ANNOUNCE':
                block = Block(req)
                if block.hash:
                    if blockchain.append(block): # auto verify on append and only adding when verified
                        target['height'] = req['height']
                        target['hash'] = req['hash']
                    else: # if not verified and save
                        Blockchain_buffer.append({'block':block, 'time':now}) # save block to buffer


            elif req['type'] == 'NEW_WORD':
                messages = req['word']
                if type(messages) == "<class 'str'>":
                    messages = [messages]
                flag = True
                if len(messages) < 1 or len(messages) > 10: # check length of messages
                    flag = False
                for message in messages:
                    if len(message) > 20: # check length of each message
                        flag = False
                if flag:
                    mining_queue.append(messages)
                else:
                    print('Removing Invalid Messages', mining_queue[0])
                    mining_queue.remove(mining_queue[0])

            elif req['type'] == 'CONSENSUS':
                consensus_timer = 0
                stats = []

