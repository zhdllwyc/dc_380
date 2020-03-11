import sys
import time
import multiprocessing 
import random
import multiprocessing.managers as mpm
from node import Node
import atexit

receiveall_called = False
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

                
def KillAll(All_Process, All_Node):
    for process in All_Process:
        process.terminate()
    for node in All_Node:
        del node

def StartMaster_Observer(All_Process, All_Node, master_queue, observer_queue):
    
    #observer only send but not receive
    ObserverSendEvent = multiprocessing.Event()
    Observer_addchannel = multiprocessing.Event()
    ObserverCollectEvent = multiprocessing.Event()
    Observer = Node(ObserverSendEvent, None, Observer_addchannel, ObserverCollectEvent, None, None, None, 'Observer', '0', 0, master_queue, observer_queue)
    #save all processes and nodes in the current process so that the master can access them
    All_Process.append(Observer.node_process)
    All_Node.append(Observer)

   
def CreateNode(Node_ID, Initial_Balance, master_queue, All_Process, All_Node, All_Queue, manager):

    Node_sendEvent = multiprocessing.Event()
    Node_receiveEvent = multiprocessing.Event()
    Node_addchannel = multiprocessing.Event()
    Node_collectEvent = multiprocessing.Event()
    Node_snapshot = multiprocessing.Event()
    Node_snapshot_finish_event = multiprocessing.Event()
    Node_receiveall_event = multiprocessing.Event()
    Current_Node = Node(Node_sendEvent, Node_receiveEvent, Node_addchannel, Node_collectEvent, Node_snapshot, Node_snapshot_finish_event, Node_receiveall_event, 'Node', Node_ID, int(Initial_Balance), master_queue,None)
    All_Process.append(Current_Node.node_process)
    All_Node.append(Current_Node)

    #set up bidirection channel with other node in the network
    for node in All_Node:
        if(node is not Current_Node):
            
            #from node to curr_node
            current_in_queue = manager.Queue()
            #from curr_node to node
            current_out_queue = manager.Queue()

            All_Queue.append([node.node_id, Node_ID, current_in_queue, current_out_queue])

            #telling node about this two queue/channel
            node.master_queue.put(Node_ID)
            node.master_queue.put(current_in_queue)
            node.master_queue.put(current_out_queue)
            #node.channel_event.set()
            if(not node.channel_event.is_set()):
                node.channel_event.set()
            else:
                while(True):
                    if(not node.channel_event.is_set()):
                        node.channel_event.set()
                        break

            #telling curr_node about this two queue/channel
            Current_Node.master_queue.put(node.node_id)
            Current_Node.master_queue.put(current_out_queue)
            Current_Node.master_queue.put(current_in_queue)
            #Current_Node.channel_event.set()
            if(not Current_Node.channel_event.is_set()):
                Current_Node.channel_event.set()
            else:
                while(True):
                    if(not Current_Node.channel_event.is_set()):
                        Current_Node.channel_event.set()
                        break


def Send(Sender_ID, Receiver_ID, Amount, All_Process, All_Node):
    #The master command tells the sender to put money in the queue:
    for node in All_Node:
        if(node.node_id == Sender_ID):
             msg = Sender_ID + " " + Receiver_ID + " " + str(Amount)
             if(not node.send_event.is_set()):
                 node.master_queue.put(msg)
                 node.send_event.set()
             else:
                #block until this message is enqueued
                 while(True):
                     if(not node.send_event.is_set()):
                         node.master_queue.put(msg)
                         node.send_event.set()
                         break


def Receive(Receiver_ID, Sender_ID, All_Process, All_Node):
    #The master command tells the receive to get message in the queue:
    for node in All_Node:
        if (node.node_id == Receiver_ID) :
            msg = Receiver_ID + " " + Sender_ID
            if(not node.receive_event.is_set()):
                node.master_queue.put(msg)
                node.receive_event.set()
            else:
                #block until this message is enqueued
                while(True):
                    if(not node.receive_event.is_set()):
                        node.master_queue.put(msg)
                        node.receive_event.set()
                        break


def DetectChannel(All_Queue):
    #Detect all non-empty channels and store them in a list
    non_empty_channel = []
    for each_queue in All_Queue:
        if each_queue[0] and each_queue[1]:
            if not each_queue[2].empty():
                Receiver_ID = each_queue[1]
                Sender_ID = each_queue[0]
                non_empty_channel.append([Receiver_ID, Sender_ID])
            if not each_queue[3].empty():
                Receiver_ID = each_queue[0]
                Sender_ID = each_queue[1]
                non_empty_channel.append([Receiver_ID, Sender_ID])
    return non_empty_channel


def ReceiveAll(All_Process, All_Node, All_Queue):
    #Drain channels of messages
    global receiveall_called
    for node in All_Node:
        if(node.node_type != 'Observer'):
            if(not node.receiveall_event.is_set()):
                node.receiveall_event.set()
            else:
                while(True):
                    if(not node.receiveall_event.is_set()):
                        node.receiveall_event.set()
                        break
       

    non_empty_channel = DetectChannel(All_Queue)
    #Call Receive until there are no empty channels
    while(len(non_empty_channel)>0):
        random_channel = random.randint(0, len(non_empty_channel)-1)

        receiver=None
        receiver_id = non_empty_channel[random_channel][0]
        for temp_node in All_Node:
            if(temp_node.node_id == non_empty_channel[random_channel][0]):
                receiver=temp_node
        Receive(receiver_id, non_empty_channel[random_channel][1], All_Process, All_Node)
        
        while True:
            if(not receiver.receive_event.is_set()):
                break
        
        non_empty_channel = DetectChannel(All_Queue)
    
    for node in All_Node:
        if(node.node_type != 'Observer'):
            node.snapshot_finish_event.clear()
            node.receiveall_event.clear()
    receiveall_called = True

def BeginSnapshot(NodeID, SendNode, All_Process, All_Node):
    #Observer sends a takesnapsot msg to the given node
    for node in All_Node:
        if(node.node_type == 'Observer'):
            msg = "TakeSnapshot" + " " + NodeID
            print("Started by Node " + str(NodeID))
            if((not SendNode.snapshot_event.is_set()) and (not node.send_event.is_set())):
                 node.master_queue.put(msg)
                 SendNode.snapshot_event.set()
                 node.send_event.set()
                 
            else:
                 while( True ):
                     if((not SendNode.snapshot_event.is_set()) and (not node.send_event.is_set())):
                         node.master_queue.put(msg)
                         SendNode.snapshot_event.set()
                         node.send_event.set()
                         break


def CollectState(All_Process, All_Node):
    #Observer collects an individual state of each node and an individual state of each channel
    global receiveall_called
    for node in All_Node:
        if(node.node_type == 'Observer'):
            msg = "Collect"
            if(not node.collect_event.is_set()):
                node.master_queue.put(msg)
                node.collect_event.set()
            else:
                while(True):
                    if(not node.collect_event.is_set()):
                        node.master_queue.put(msg)
                        node.collect_event.set()
                        break
   
    while(True):
        if(receiveall_called):
            break
    
    for node in All_Node:
        if(node.node_type != 'Observer'):
            if( (not node.collect_event.is_set()) and (not node.receive_event.is_set() ) and (not node.receiveall_event.is_set() )):
                node.collect_event.set()
            else:
                while(True):
                    if((not node.collect_event.is_set()) and (not node.receive_event.is_set() ) and (not node.receiveall_event.is_set() )):
                        node.collect_event.set()
                        break

    receiveall_called = False


def PrintSnapshot(All_Node, observer_queue):
    #Print out the collected global state
    N = len(All_Node)
    all_node_state = [0] * N
    all_channel_state = [[0]*N for i in range(N)]
    observer_msg = None
    while(True):
        #print(observer_queue.empty())
        #time.sleep(0.1)
        if((not observer_queue.empty()) ): 
            observer_msg = observer_queue.get()
            break
    
    for nodeid in observer_msg:
        all_node_state[int(nodeid)] = observer_msg[nodeid][0]
        #observer_msg[nodeid][1]=channel_state
        for node_channel in observer_msg[nodeid][1]:
            total_amount = 0
            for transaction in observer_msg[nodeid][1][node_channel]:
                total_amount=total_amount+int(transaction)
            all_channel_state[int(node_channel)][int(nodeid)] = total_amount 
    '''
    for each_queue in All_Queue:
        # print(each_queue[0], each_queue[1], each_queue[2].get(), each_queue[3].get())
        if(each_queue[0] == "0") and (not each_queue[3].empty()):
            node_id = int(each_queue[1])
            [node_state, channel_state] = each_queue[3].get()
        elif(each_queue[1] == "0") and (not each_queue[2].empty()):
            node_id = int(each_queue[0])
            [node_state, channel_state] = each_queue[2].get()
        else:
            continue
        all_node_state[node_id] = node_state
        for sender in channel_state:
            all_channel_state[int(sender)][node_id] = sum(channel_state[sender])
    '''
    print("---Node states")
    for i in range(1, N):
        print("Node" + " " + str(i) + " = " + str(all_node_state[i]))

    print("---Channel states")
    for i in range(1, N):
        for j in range(1, N):
            if i != j:
                print("channel" + "(" + str(i) + "->" + str(j) + ")" + " = " + str(all_channel_state[i][j]))



def argument_parsing(current_command_list, All_Process, All_Node, All_Queue, manager, observer_queue):
    
    if(current_command_list[0] == 'StartMaster'):
        # connection between master and observer, unidirection queue, only master to observer
        master_queue = manager.Queue()
        StartMaster_Observer(All_Process, All_Node, master_queue, observer_queue)

    elif(current_command_list[0] == 'CreateNode'):
        # connection between master and observer, unidirection queue, only master to observer
        master_queue = manager.Queue()
        CreateNode(current_command_list[1], current_command_list[2], master_queue, All_Process, All_Node, All_Queue, manager)

    elif(current_command_list[0] == 'Send'):
        # send receiver money
        Send(current_command_list[1], current_command_list[2], current_command_list[3], All_Process, All_Node)

    elif(current_command_list[0] == 'Receive'):
        # receiver receive money or takesnapshot
        Receiver = current_command_list[1]
        Sender = "None"
        if(len(current_command_list) > 2):
            Sender = current_command_list[2]
        Receive(Receiver, Sender, All_Process, All_Node)

    elif(current_command_list[0] == 'BeginSnapshot'):
        
        # tell observer to begin snapshot
        SendNode = None
        for node in All_Node:
            if( node.node_id == current_command_list[1] ):
                SendNode = node
        BeginSnapshot(current_command_list[1], SendNode, All_Process, All_Node)

    elif(current_command_list[0] == 'KillAll'):
        # kill all processes
        KillAll(All_Process, All_Node)

    elif(current_command_list[0] == 'ReceiveAll'):
        # drain all channels
        ReceiveAll(All_Process, All_Node, All_Queue)

    elif(current_command_list[0] == 'CollectState'):
        # tell observer to collect the state from nodes
        CollectState(All_Process, All_Node)

    elif(current_command_list[0] == 'PrintSnapshot'):
        # print snapshot
        PrintSnapshot(All_Node, observer_queue)


if __name__ == "__main__": 
    
    #our master is our current process

    #a list of all alive process
    All_Process = []
    #a list of all nodes
    All_Node = []
    #a list of all queues
    All_Queue = []
    manager = multiprocessing.Manager()
        
    arg_file= sys.argv[1]
  
    command_list = []
    with open(arg_file) as af:
        line = af.readline()
        while line:
            temp_arg = line.split()
            command_list.append(temp_arg)
            line = af.readline()
    af.close()

    #obserer to master
    observer_queue = manager.Queue()
    for command in command_list:
        print(" ".join(command))
        argument_parsing(command, All_Process, All_Node, All_Queue, manager,observer_queue)
        #time.sleep(0.1)
        
    time.sleep(2)
    KillAll(All_Process,All_Node)



