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
    def __init__(self, send_event, receive_event, channel_event, node_type, Node_ID, Initial_Balance, master_queue):
        
        self.node_type = node_type
        self.node_id = Node_ID
        self.balance = Initial_Balance

        #The send and receive event this node is listening upon, master only has send
        #add channel event for master notify setting up channel with newly created node 
        self.send_event = send_event
        self.receive_event = receive_event
        self.channel_event = channel_event
        #the channel between given node and the master
        self.master_queue = master_queue

        #start process
        if(self.node_type == 'Observer'):
            self.node_process = multiprocessing.Process(target=self.observer_notify, args=(master_queue, send_event, receive_event, channel_event, node_type, Node_ID, Initial_Balance))
        else: 
            self.node_process = multiprocessing.Process(target=self.node_notify, args=(master_queue, send_event, receive_event, channel_event, node_type, Node_ID, Initial_Balance))
       
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
                out_channel =  self.master_queue.get()
                in_channel =  self.master_queue.get()
                self.in_queue[new_node_id] = in_channel
                self.out_queue[new_node_id] = out_channel
                print("observer")
                print(self.node_id)
                print(new_node_id)
                print(self.in_queue)
                print(self.out_queue)
                self.channel_event.clear()
            #if(self.send_event.is_set()):

        

    def node_notify(self, master_queue, send_event, receive_event, channel_event, node_type, Node_ID, Initial_Balance):
        #need to reinitialize because in different memory space
        self.node_type = node_type
        self.node_id = Node_ID
        self.balance = Initial_Balance

        self.send_event = send_event
        self.receive_event = receive_event
        self.channel_event = channel_event

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
                out_channel =  self.master_queue.get()
                in_channel =  self.master_queue.get()
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

                    for node_ID in self.out_queue:
                        if(node_ID == receiver):
                            print(self.out_queue[node_ID])
                            send_msg = str(amount)
                            print(send_msg)
                            self.out_queue[node_ID].put(send_msg)
                self.send_event.clear()
            

                
def KillAll(All_Process, All_Node):
    for process in All_Process:
        process.terminate()
    for node in All_Node:
        del node

def StartMaster_Observer(All_Process, All_Node, master_queue):
    
    #observer only send but not receive
    ObserverSendEvent = multiprocessing.Event()
    Observer_addchannel = multiprocessing.Event()
    Observer = Node(ObserverSendEvent, None, Observer_addchannel, 'Observer', 0, 0, master_queue)
    All_Process.append(Observer.node_process)
    All_Node.append(Observer)

   
def CreateNode(Node_ID, Initial_Balance, master_queue, All_Process, All_Node, manager):

    Node_sendEvent = multiprocessing.Event()
    Node_receiveEvent = multiprocessing.Event()
    Node_addchannel = multiprocessing.Event()
    Current_Node = Node(Node_sendEvent, Node_receiveEvent, Node_addchannel, 'Node', Node_ID, int(Initial_Balance), master_queue)
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
             node.master_queue.put(msg)
             if(not node.send_event.is_set()):
                 node.send_event.set()
             else:
                 while(node.send_event.is_set()):
                     if(not node.send_event.is_set()):
                         node.send_event.set()
                         break
             

'''
def Receive(Receiver_ID, Sender_ID, All_Process, All_Node):
'''

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
        #Receive(current_command_list[1], current_command_list[2], All_Process, All_Node)

    elif(current_command_list[0] == 'BeginSnapshot'):
        print('BeginSnapshot')

    elif(current_command_list[0] == 'KillAll'):
        print('KillAll')
        KillAll(All_Process, All_Node)

    elif(current_command_list[0] == 'ReceiveAll'):
        print('ReceiveAll')

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






