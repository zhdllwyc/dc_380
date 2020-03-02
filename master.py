import sys
import time
import multiprocessing 
import multiprocessing.managers as mpm


#patching python3 bug
backup_autoproxy = mpm.AutoProxy

def AutoProxy(token, serializer, manager=None, authkey=None,
              exposed=None, incref=True, manager_owned=False):
    
    _Client = mpm.listener_client[serializer][1]

    if exposed is None:
        conn = _Client(token.address, authkey=authkey)
        try:
            exposed = dispatch(conn, None, 'get_methods', (token,))
        finally:
            conn.close()

    if authkey is None and manager is not None:
        authkey = manager._authkey
    if authkey is None:
        authkey = multiprocessing.process.current_process().authkey

    ProxyType = mpm.MakeProxyType('AutoProxy[%s]' % token.typeid, exposed)
    proxy = ProxyType(token, serializer, manager=manager, authkey=authkey,
                      incref=incref, manager_owned=manager_owned)
    proxy._isauto = True
    return proxy

mpm.AutoProxy = AutoProxy


#a class for node
class Node:
    def __init__(self, send_event, receive_event, channel_event, snapshot_event, node_type, Node_ID, Initial_Balance, master_queue):
        
        self.node_type = node_type
        self.node_id = Node_ID
        self.balance = Initial_Balance

        #The send and receive event this node is listening upon, master only has send
        #add channel event for master notify setting up channel with newly created node 
        #takesnapshot event for whether or not master can tell the observer to send a given node snapshot msg
        self.send_event = send_event
        self.receive_event = receive_event
        self.channel_event = channel_event
        self.snapshot_event = snapshot_event
        #the channel between given node and the master
        self.master_queue = master_queue

        #start process
        if(self.node_type == 'Observer'):
            self.node_process = multiprocessing.Process(target=self.observer_notify, args=(master_queue, send_event, receive_event, channel_event, node_type, Node_ID, Initial_Balance))
        else: 
            self.node_process = multiprocessing.Process(target=self.node_notify, args=(master_queue, send_event, receive_event, channel_event, snapshot_event, node_type, Node_ID, Initial_Balance))
       
        self.node_process.start()
        
    

    def observer_notify(self, master_queue, send_event, receive_event, channel_event, node_type, Node_ID, Initial_Balance):

        #need to reinitialize because in different memory space
        self.node_type = node_type
        self.node_id = Node_ID
        self.balance = Initial_Balance

        self.send_event = send_event
        self.receive_event = receive_event
        self.channel_event = channel_event

        #connection from master to observer
        self.master_queue = master_queue
        # a dictionary of incomming queue/channel, key: other node in the network, value: channel
        self.in_queue = {}
        # a dictionary of outgoing queue/channel, key: other node in the network, value: channel
        self.out_queue = {}
        
        while True:
            
            #waiting to create new channel with new node
            if(self.channel_event.is_set()):            
                new_node_id = self.master_queue.get()
                out_channel = self.master_queue.get()
                in_channel = self.master_queue.get()
                self.in_queue[new_node_id] = in_channel
                self.out_queue[new_node_id] = out_channel
                print("observer")
                print(self.node_id)
                print(new_node_id)
                print(self.in_queue)
                print(self.out_queue)
                self.channel_event.clear()
            
            #sending snapshot message
            if(self.send_event.is_set()):
                msg = self.master_queue.get().split()
                sender_id = msg[1]
                self.out_queue[sender_id].put("TakeSnapshot")
                self.send_event.clear()
        

    def node_notify(self, master_queue, send_event, receive_event, channel_event, snapshot_event, node_type, Node_ID, Initial_Balance):

        #indicating if this node ever receive a snapshot command
        #saved state when receive snapshot msg
        #channel state for chandy-lamport
        first_snap = True
        save_state = 0
        channel_state = {}
        incoming_node_id = 0
        #need to reinitialize because in different memory space
        self.node_type = node_type
        self.node_id = Node_ID
        self.balance = Initial_Balance

        self.send_event = send_event
        self.receive_event = receive_event
        self.channel_event = channel_event
        self.snapshot_event = snapshot_event
        #connection from master to this node/process
        self.master_queue = master_queue
        # a dictionary of incomming queue/channel, key: other node in the network, value: channel
        self.in_queue = {}
        # a dictionary of outgoing queue/channel, key: other node in the network, value: channel
        self.out_queue = {}

        while True:
            #waiting to create new channel with new node
            if(self.channel_event.is_set()):
                new_node_id = self.master_queue.get()
                out_channel = self.master_queue.get()
                in_channel = self.master_queue.get()
                self.in_queue[new_node_id] = in_channel
                self.out_queue[new_node_id] = out_channel
                
                print(self.node_id)
                print(self.in_queue)
                print(self.out_queue)
                self.channel_event.clear()

            if(self.send_event.is_set()):
                msg = self.master_queue.get().split()
                receiver = msg[1]
                amount = int(msg[2])
                print(amount)
                if(amount > self.balance):
                    print("ERR_SEND")
                else:
                    print(self.balance)
                    self.balance=self.balance-amount
                    print(self.balance)

                    print(self.out_queue[receiver])
                    send_msg = str(amount)
                    print(send_msg)
                    self.out_queue[receiver].put(send_msg)
                self.send_event.clear()
            

            #upon receiving 
            if(self.receive_event.is_set()):
                msg = self.master_queue.get().split()
                receiver = msg[0]
                sender = msg[1]
                retrieve = None
                if (sender!="None"):
                    retrieve = self.in_queue[sender].get()
                else:
                    rand_sender = random.choice(list(self.in_queue))
                    retrieve = self.in_queue[rand_sender].get()
                
                if(retrieve == "TakeSnapshot"):
                    if(first_snap):
                        first_snap = False
                        save_state = self.balance
                        incoming_node_id = sender
                        print(retrieve)
                        msg = "TakeSnapshot"
                        for other_node_id in self.out_queue:
                            self.out_queue[other_node_id].put(msg)
                            channel_state[other_node_id] = []

                else:
                    print(self.balance)
                    self.balance=self.balance+int(retrieve)
                    print(self.balance)
                    if((first_snap == False) and (sender!=incoming_node_id)):
                        channel_state[sender].append(retrieve)
                
                self.receive_event.clear()
            
            #polling on the observer incoming channel, this is the node the observer send the snapshot msg
            if(len(self.in_queue)!=0):
                if(not self.in_queue[0].empty()):
                    if(first_snap):
                        first_snap = False
                        save_state = self.balance
                        print(self.in_queue[0].get())
                        msg = "TakeSnapshot"
                        for other_node_id in self.out_queue:
                            self.out_queue[other_node_id].put(msg)
                            channel_state[other_node_id] = []
                        self.snapshot_event.clear()
                
def KillAll(All_Process, All_Node):
    for process in All_Process:
        process.terminate()
    for node in All_Node:
        del node

def StartMaster_Observer(All_Process, All_Node, master_queue):
    
    #observer only send but not receive
    ObserverSendEvent = multiprocessing.Event()
    Observer_addchannel = multiprocessing.Event()
    Observer = Node(ObserverSendEvent, None, Observer_addchannel, None, 'Observer', 0, 0, master_queue)
    print(type(Observer.node_id))
    All_Process.append(Observer.node_process)
    All_Node.append(Observer)

   
def CreateNode(Node_ID, Initial_Balance, master_queue, All_Process, All_Node, manager):

    Node_sendEvent = multiprocessing.Event()
    Node_receiveEvent = multiprocessing.Event()
    Node_addchannel = multiprocessing.Event()
    Node_snapshot = multiprocessing.Event()
    Current_Node = Node(Node_sendEvent, Node_receiveEvent, Node_addchannel, Node_snapshot, 'Node', Node_ID, int(Initial_Balance), master_queue)
    All_Process.append(Current_Node.node_process)
    All_Node.append(Current_Node)

    #set up bidirection channel with other node in the network
    for node in All_Node:
        if(node is not Current_Node):
            
            #from node to curr_node
            current_in_queue = manager.Queue()
            #from curr_node to node
            current_out_queue = manager.Queue()

            #telling node about this two queue/channel
            node.master_queue.put(Node_ID)
            node.master_queue.put(current_in_queue)
            node.master_queue.put(current_out_queue)
            node.channel_event.set()

            #telling curr_node about this two queue/channel
            Current_Node.master_queue.put(node.node_id)
            Current_Node.master_queue.put(current_out_queue)
            Current_Node.master_queue.put(current_in_queue)
            Current_Node.channel_event.set()


def Send(Sender_ID, Receiver_ID, Amount, All_Process, All_Node):
    #The master command tells the sender to put money in the queue:
    for node in All_Node:
        if(node.node_id == Sender_ID):
             msg = Sender_ID + " " + Receiver_ID + " " + str(Amount)
             if(not node.send_event.is_set()):
                 node.master_queue.put(msg)
                 node.send_event.set()
             else:
                 while(node.send_event.is_set()):
                     if(not node.send_event.is_set()):
                         node.master_queue.put(msg)
                         node.send_event.set()
                         break


def Receive(Receiver_ID, Sender_ID, All_Process, All_Node):
    for node in All_Node:
        if (node.node_id == Receiver_ID):
            msg = Receiver_ID + " " + Sender_ID
            if(not node.receive_event.is_set()):
                 node.master_queue.put(msg)
                 node.receive_event.set()
            else:
                 while(node.receive_event.is_set()):
                     if(not node.receive_event.is_set()):
                         node.master_queue.put(msg)
                         node.receive_event.set()
                         break


# def ReceiveAll(All_Process, All_Node):
#     for node in All_Node:

def BeginSnapshot(NodeID, SendNode, All_Process, All_Node):
    #Observer sends a takesnapsot msg to the given node
    for node in All_Node:
        if (node.node_type == 'Observer'):
            msg = "TakeSnapshot" + " " + NodeID
            print(msg)
            if((not SendNode.snapshot_event.is_set()) and (not node.send_event.is_set())):
                 print("send")
                 node.master_queue.put(msg)
                 SendNode.snapshot_event.set()
                 node.send_event.set()
                 
            else:
                 while( (SendNode.snapshot_event.is_set()) or (node.send_event.is_set())):
                     if((not SendNode.snapshot_event.is_set()) and (not node.send_event.is_set())):
                         node.master_queue.put(msg)
                         SendNode.snapshot_event.set()
                         node.send_event.set()
                         break




def argument_parsing(current_command_list, All_Process, All_Node, manager):
    if(current_command_list[0] == 'StartMaster'):
        print('StartMaster')
        #connection between master and observer, unidirection queue, only master to observer
        master_queue = manager.Queue()
        StartMaster_Observer(All_Process, All_Node, master_queue)

    elif(current_command_list[0] == 'CreateNode'):
        print('CreateNode')
        #connection between master and observer, unidirection queue, only master to observer
        master_queue = manager.Queue()
        CreateNode(current_command_list[1], current_command_list[2], master_queue, All_Process, All_Node, manager)

    elif(current_command_list[0] == 'Send'):
        print('Send')
        # sender receiver money
        Send(current_command_list[1], current_command_list[2], current_command_list[3], All_Process, All_Node)

    elif(current_command_list[0] == 'Receive'):
        print('Receive')
        Receiver = current_command_list[1]
        Sender = "None"
        if(len(current_command_list) > 2):
            Sender = current_command_list[2]
        Receive(Receiver, Sender, All_Process, All_Node)

    elif(current_command_list[0] == 'BeginSnapshot'):
        print('BeginSnapshot')
        SendNode = None
        for node in All_Node:
            if( node.node_id == current_command_list[1] ):
                SendNode = node
        BeginSnapshot(current_command_list[1], SendNode, All_Process, All_Node)

    elif(current_command_list[0] == 'KillAll'):
        print('KillAll')
        KillAll(All_Process, All_Node)

    elif(current_command_list[0] == 'ReceiveAll'):
        print('ReceiveAll')
        ReceiveAll(All_Process, All_Node)

    elif(current_command_list[0] == 'CollectState'):
        print('CollectState')

    elif(current_command_list[0] == 'PrintSnapshot'):
        print('PrintSnapshot')

if __name__ == "__main__": 

    #our master is our current process

    #a list of all alive process
    All_Process = []
    #a list of all nodes
    All_Node = []
    manager = multiprocessing.Manager()
    while( True ):
        user_input = input ("User input :")
        current_command_list = user_input.split()
        argument_parsing(current_command_list, All_Process, All_Node, manager)
           




























    '''
    arg_file= sys.argv[1]
    print(arg_file)
  
    command_list = []
    with open(arg_file) as af:
        line = af.readline()
        while line:
            temp_arg = line.split()
            command_list.append(temp_arg)
            line = af.readline()
    print(command_list)
    '''






