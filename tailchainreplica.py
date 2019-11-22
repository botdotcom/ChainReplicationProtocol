# all imports
import chain_pb2
import chain_pb2_grpc

import grpc
import time
import sys
from concurrent import futures

from kazoo.client import KazooClient
from kazoo.retry import KazooRetry
from kazoo.exceptions import KazooException, ZookeeperError
import logging

logging.basicConfig()


class TailChainReplicaServicer(chain_pb2_grpc.TailChainReplicaServicer):
    def __init__(self, host, port, ishead=False, istail=False, previousid=0, nextid=0, chaintailid=0):
        self.inet = "{}:{}".format(host, port)
        self.retry = KazooRetry(max_tries=1000, delay=0.5)
        self.zk = KazooClient(hosts=self.inet, logger=logging, connection_retry=self.retry)
        
        self.ishead = ishead    # am I head?
        self.istail = istail    # am I tail?
        self.replicaid = 0  # id of this replica
        self.previousid = previousid    # id of predecessor
        self.nextid = nextid    # id of successor
        self.chaintailid = chaintailid
        self.hashtable = {} # table of values updated by client requests
        self.client  = {}   # client stubs for future use
        self.sentlist = []  # sent update requests not yet processed by tail
        self.previous = None
        self.next = None
        self.chaintail = None
        self.currxid = 0    # current xid server has seen so far
        self.host = host
        self.port = port

    # grpc functions
    # propagate state requests
    def proposeStateUpdate(self, request, context):
        """
        1. request contains source, xid, key, value, host, port and cxid
        2. update the state of this replica
        3. if not tail, process and forward
        """
        # making stub to connect to successor
        src = request.src
        if src == self.previousid:
            xid = request.xid
            key = request.key
            value = request.value
            host = request.host
            port = request.port
            cxid = request.cxid

            print("TailStateUpdateRequest received: xid = {}, key = {}, value = {}".format(xid, key, value))
            self.hashtable[key] = value
            currxid = xid
            
            if self.istail:
                # reply to client
                if self.client.get(host+":"+port) is None:
                    self.client[host+":"+port] = chain_pb2_grpc.TailClientStub(channel)
                cxidrequest = chain_pb2.CxidProcessedRequest(cxid=cxid)
                # cxidprocessed?

            else:
                # forward state to successor if not tail
                with grpc.insecure_channel("{}:{}".format(host, port)) as channel:
                    stub = chain_pb2_grpc.TailChainReplicaStub(channel)
                    response = stub.proposeStateUpdate(chain_pb2.TailStateUpdateRequest(src = self.replicaid,
                    xid = xid,
                    key = key,
                    value = value,
                    host = host, 
                    port = port,
                    cxid = cxid))
                    self.next.proposeStateUpdate(response)
                    self.sentlist.append(response)

                    # respond 
                    return chain_pb2.ChainResponse(rc=0)  # success
        else:
            print("Improper TailStateUpdateRequest received")
            return chain_pb2.ChainResponse(rc=1)  # you're not supposed to talk to me

    # sent by client, only tail responds
    def getLatestXid(self, request, context):
        """
        send back xid if the replica is currently tail
        """
        if self.istail:
            print("LatestXidRequest received")
            return chain_pb2.LatestXidResponse(rc=0, xid=self.currxid)
        else:
            print("Improper LatestXidRequest received")
            return chain_pb2.ChainResponse(rc=1)  # you're not supposed to talk to me

    # transfer state
    def stateTransfer(self, request, context):
        """
        1. receives request for state transfer
        2. transfers source, state and some state update requests
        """
        src = request.src
        if src == self.previousid:
            stateXid = request.stateXid
            statemap = request.state
            allsent = request.sent

            print("TailStateTransferRequest received")
            for sent in allsent:
                xid = sent.xid
                key = sent.key
                value = sent.value
                host = sent.host
                port = sent.port
                cxid = sent.cxid

                if xid > self.currxid:
                    self.currxid = xid
                    if self.istail:
                        # reply to client
                        if self.client.get(host+":"+port) is None:
                            self.client[host+":"+port] = chain_pb2_grpc.TailClientStub(channel)
                        cxidrequest = chain_pb2.CxidProcessedRequest(cxid=cxid)
                        # cxidprocessed?
                    else:
                        # forward state update to successor if not tail
                        with grpc.insecure_channel("{}:{}".format(host, port)) as channel:
                            stub = chain_pb2_grpc.TailChainReplicaStub(channel)
                            response = stub.stateTransfer(chain_pb2.TailStateTransferRequest(src = self.currxid,
                            xid = xid,
                            key = key,
                            value = value,
                            host = host, 
                            port = port,
                            cxid = cxid))

                            self.next.stateTransfer(response)
                            self.sentlist.append(response)

                            # respond 
                            return chain_pb2.ChainResponse(rc=0)  # success

        else:
            print("Improper TailStateTransferRequest received")
            return chain_pb2.ChainResponse(rc=1)  # you're not supposed to talk to me

    # add/modify key of hash table
    def increment(self, request, context):
        if self.ishead:
            key = request.key
            value = request.incrValue
            host = request.host
            port = request.port
            cxid = request.cxid
            print("TailIncrementRequest received: cxid = {}, key = {}, value = {}".format(cxid, key, value))

            v = 0
            if self.hashtable[key]:
                v = self.hashtable[key]
            self.hashtable[key] = v + value
            self.currxid += 1

            if self.istail:
                # reply to client
                if self.client.get(host+":"+port) is None:
                    self.client[host+":"+port] = chain_pb2_grpc.TailClientStub(channel)
                cxidrequest = chain_pb2.CxidProcessedRequest(cxid=cxid)
                # cxidprocessed?

            else:
                # forward state to successor if not tail
                with grpc.insecure_channel("{}:{}".format(host, port)) as channel:
                    stub = chain_pb2_grpc.TailChainReplicaStub(channel)
                    response = stub.proposeStateUpdate(chain_pb2.TailStateUpdateRequest(src = self.currxid,
                    xid = xid,
                    key = key,
                    value = self.hashtable[key],
                    host = host, 
                    port = port,
                    cxid = cxid))
                    self.next.proposeStateUpdate(response)
                    self.sentlist.append(response)

                    return chain_pb2.HeadResponse(rc=0) # success

        else:
            print("Improper TailIncrementRequest received")
            return chain_pb2.HeadResponse(rc=1)  # you're not supposed to talk to me

    def delete(self, request, context):
        if self.ishead:
            key = request.key
            host = request.host
            port = request.port
            cxid = request.cxid
            print("TailDeleteRequest received: cxid = {}, key = {}, value = {}".format(cxid, key, value))

            self.hashtable[key] = 0
            self.currxid += 1

            if self.istail:
                # reply to client
                if self.client.get(host+":"+port) is None:
                    self.client[host+":"+port] = chain_pb2_grpc.TailClientStub(channel)
                cxidrequest = chain_pb2.CxidProcessedRequest(cxid=cxid)
                # cxidprocessed?

            else:
                # forward state to successor if not tail
                with grpc.insecure_channel("{}:{}".format(host, port)) as channel:
                    stub = chain_pb2_grpc.TailChainReplicaStub(channel)
                    response = stub.proposeStateUpdate(chain_pb2.TailStateUpdateRequest(src = self.currxid,
                    xid = xid,
                    key = key,
                    value = self.hashtable[key],
                    host = host, 
                    port = port,
                    cxid = cxid))
                    self.next.proposeStateUpdate(response)
                    self.sentlist.append(response)

                    return chain_pb2.HeadResponse(rc=0) # success

        else:
            print("Improper TailDeleteRequest received")
            return chain_pb2.HeadResponse(rc=1)  # you're not supposed to talk to me

    def get(self, request, context):
        if self.istail:
            key = request.key
            value = 0
            print("GetRequest received for key = {}".format(key))

            if self.hashtable[key]:
                value = self.hashtable[key]
            else:
                self.hashtable[key] = 0
            return chain_pb2.GetResponse(rc=0, value=value)  # success
        else:
            print("Improper GetRequest received")
            return chain_pb2.GetResponse(rc=1, value=0)  # you're not supposed to talk to me


    # helper functions
    def setpreviouschain(self, id):
        if self.previousid == id:
            return
        self.previousid = id
        # assign stub to self.previous here

    def setnextchain(self, id):
        if self.nextid == id:
            return
        self.nextid = id
        # assign stub to self.next here
        # transfer state

    def setchaintail(self, id):
        if self.chaintailid == id:
            return
        self.chaintailid = id
        # assign stub to self.chaintail here
    
    def zooconnection(self):
        clientid = 0

        try:
            self.zk.start()
            self.zk.ensure_path('/tailchain')
            clientid = hex(self.zk.client_id[0])
            self.replicaid = clientid
            print("Connected to Zookeeper with ID = {}".format(clientid))
            self.zk.create("/tailchain/{}".format(clientid), b'sss', ephemeral=True)   # adding my ephemeral replica to the chain
        except KazooException:
            print("Could not connect to Zookeeper")

        children = self.zk.get_children('/tailchain', watch=self.zooconnection)
        # sort replicas to get head and tail
        children.sort()

        # check if head
        if clientid == children[0]:
            self.ishead = True
        else:
            self.ishead = False

        # check if tail, else set connectin with tail
        if clientid == children[-1]:
            self.istail = True
        else:
            self.istail = False
        self.setchaintail(clientid)

        # get predecessor and successor replicas
        index = 0
        try:
            index = children.index(clientid)
        except ValueError:
            print("Not found in children list!")

        if index > 0:
            self.setpreviouschain(index-1)
        else:
            self.previous = None

        if index < (len(children)-1):
            self.setnextchain(index+1)
        else:
            self.next = None

        print("-----")  
        print(children)
        print("Is head: {}, Head: {}".format(self.ishead, children[0]))
        print("Is tail: {}, Tail: {}".format(self.istail, children[-1]))
        print("Predecessor: {}".format(self.previousid))
        print("Successor: {}".format(self.nextid))
            
        
    def starter(self):
        try:
            server = grpc.server(futures.ThreadPoolExecutor())
            chain_pb2_grpc.add_TailChainReplicaServicer_to_server(self, server)
            server.add_insecure_port("[::]:{}".format(self.port))

            # start replica
            server.start()
            time.sleep(1)
            print("Replica is up and running...")

            # get zookeeper running
            self.zooconnection()
        except KeyboardInterrupt:
            print("Disconnecting from zookeeper...")
            self.zk.delete('/tailchain/{}'.format(clientid), recursive=True)
            print("Disconnected...")
            print("Stopping replica manually...")
            server.stop(None)
            print("Replica stopped...")
        


host = sys.argv[1]
port = sys.argv[2]
node = TailChainReplicaServicer(host=host, port=port).starter()