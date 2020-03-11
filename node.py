import sys
import time
import copy
import multiprocessing 
import random
import multiprocessing.managers as mpm

#a class for node
class Node:
    def __init__(self, send_event, receive_event, channel_event, collect_event, snapshot_event, snapshot_finish_event, receiveall_event, node_type, Node_ID, Initial_Balance, master_queue, observer_queue):
        
        self.node_type = node_type
        self.node_id = Node_ID
        self.balance = Initial_Balance

        #The send and receive event this node is listening upon, master only has send
        #add channel event for master notify setting up channel with newly created node 
        #takesnapshot event for whether or not master can tell the observer to send a given node snapshot msg
        self.send_event = send_event
        self.receive_event = receive_event
        self.channel_event = channel_event
        self.collect_event = collect_event
        self.snapshot_event = snapshot_event
        self.receiveall_event = receiveall_event
        self.snapshot_finish_event = snapshot_finish_event
        #the channel between given node and the master
        self.master_queue = master_queue

        #start process
        if(self.node_type == 'Observer'):
            self.node_process = multiprocessing.Process(target=self.observer_notify, args=(master_queue, observer_queue, send_event, receive_event, channel_event, collect_event, node_type, Node_ID, Initial_Balance))
        else: 
            self.node_process = multiprocessing.Process(target=self.node_notify, args=(master_queue, send_event, receive_event, channel_event, collect_event, snapshot_event, snapshot_finish_event, receiveall_event, node_type, Node_ID, Initial_Balance))
       
        self.node_process.start()
        
    

    def observer_notify(self, master_queue, observer_queue, send_event, receive_event, channel_event, collect_event, node_type, Node_ID, Initial_Balance):

        #need to reinitialize because in different memory space
        self.node_type = node_type
        self.node_id = Node_ID
        self.balance = Initial_Balance

        self.send_event = send_event
        self.receive_event = receive_event
        self.channel_event = channel_event
        self.collect_event = collect_event
        #connection from master to observer
        self.master_queue = master_queue
        self.observer_queue = observer_queue
        # a dictionary of incomming queue/channel, key: other node in the network, value: channel
        self.in_queue = {}
        # a dictionary of outgoing queue/channel, key: other node in the network, value: channel
        self.out_queue = {}
        #for observer to collect state
        final_state={}
        copy_final_state={}
        count_msg = 0
        collect_sent=False
        while True:
            
            #waiting to create new channel with new node
            if(self.channel_event.is_set()):            
                new_node_id = self.master_queue.get()
                out_channel = self.master_queue.get()
                in_channel = self.master_queue.get()
                self.in_queue[new_node_id] = in_channel
                self.out_queue[new_node_id] = out_channel
                self.channel_event.clear()
            
            #sending snapshot message
            if(self.send_event.is_set()):
                msg = self.master_queue.get().split()
                sender_id = msg[1]
                self.out_queue[sender_id].put("TakeSnapshot")
                self.send_event.clear()

            if((collect_sent==False) and (self.collect_event.is_set())):
                msg = self.master_queue.get()
                if msg == "Collect":
                    for node_id in self.out_queue:
                        self.out_queue[node_id].put(msg)
                collect_sent = True
                
            for nodeid in self.in_queue:
                if(not self.in_queue[nodeid].empty()):
                    # print("counting ", count_msg)
                    count_msg = count_msg+1
                    msg = self.in_queue[nodeid].get()
                    final_state[nodeid] = msg
            
            if( (not(len(self.in_queue)==0)) and (count_msg == len(self.in_queue)) ):
                count_msg=0
                copy_final_state = copy.deepcopy(final_state)
                print("master:" )
                print(copy_final_state)
                self.observer_queue.put(copy_final_state)
                final_state={}
                self.collect_event.clear()
                collect_sent = False
            


    def node_notify(self, master_queue, send_event, receive_event, channel_event, collect_event, snapshot_event, snapshot_finish_event, receiveall_event, node_type, Node_ID, Initial_Balance):

        #indicating if this node ever receive a snapshot command
        #saved state when receive snapshot msg
        #channel state for chandy-lamport
        #number_of_snapshot = n-1 means the node finishes snapshoting
        first_snap = True
        save_state = 0
        channel_state = {}
        final_channel_state={}
        incoming_node_id = 0
        number_of_snapshot = 0
        #need to reinitialize because in different memory space
        self.node_type = node_type
        self.node_id = Node_ID
        self.balance = Initial_Balance

        self.send_event = send_event
        self.receive_event = receive_event
        self.channel_event = channel_event
        self.collect_event = collect_event
        self.snapshot_event = snapshot_event
        self.receiveall_event = receiveall_event
        self.snapshot_finish_event = snapshot_finish_event
        self.snapshot_finish_event.clear()
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
                self.channel_event.clear()

            if(self.send_event.is_set()):
                
                msg = self.master_queue.get().split()
                receiver = msg[1]
                amount = int(msg[2])
                if(amount > self.balance):
                    print("ERR_SEND")
                else:
                    self.balance=self.balance-amount
                    send_msg = str(amount)
                    self.out_queue[receiver].put(send_msg)
                
                self.send_event.clear()

            #upon receiving 
            if(self.receive_event.is_set()):
                msg = self.master_queue.get().split()
                receiver = msg[0]
                sender = msg[1]
                retrieve = None
                
                if (sender!="None" and (not self.in_queue[sender].empty()) ):
                    retrieve = self.in_queue[sender].get()
                else:
                    sender = random.choice(list(self.in_queue.keys())[1:])
                    if(not self.in_queue[sender].empty()):
                        retrieve = self.in_queue[sender].get()
                
                '''
                if(self.in_queue[sender].empty()):
                    self.receive_event.clear()
                    continue
                '''
                if(retrieve == None):
                    self.receive_event.clear()
                    continue
                if(retrieve == "TakeSnapshot"):
                    number_of_snapshot = number_of_snapshot+1
                    if(first_snap): 
                        first_snap = False
                        save_state = self.balance
                        incoming_node_id = sender
                        if not receiveall_event.is_set(): print(sender+ " " +retrieve+ " " +"-1")
                        msg = "TakeSnapshot"
                        channel_state={}
                        for other_node_id in self.out_queue:
                            if other_node_id != '0':
                                self.out_queue[other_node_id].put(msg)
                                channel_state[other_node_id] = []
                    if(number_of_snapshot == (len(self.out_queue)-1)):
                        # print(self.node_id, "SNAPSHOT FINISHED")
                        self.snapshot_finish_event.set()
                        final_channel_state = copy.deepcopy(channel_state)
                        number_of_snapshot=0
                else:
                    if not receiveall_event.is_set(): print(sender+ " " +"Transfer"+ " " +retrieve)
                    self.balance=self.balance+int(retrieve)
                    if((first_snap == False) and (sender!=incoming_node_id)):
                        channel_state[sender].append(int(retrieve))
                        
                
                self.receive_event.clear()
            
            
            #sending own saved state and channel state to observer
            if(self.collect_event.is_set()):
                if(self.in_queue['0'].get() == "Collect"):
                    msg = [save_state, final_channel_state]
                    self.out_queue['0'].put(msg)
                    self.collect_event.clear()
                    self.snapshot_finish_event.clear()
                    self.snapshot_event.clear()
                    save_state=0
                    incoming_node_id=0
                    first_snap = True

            #polling on the observer incoming channel, this is the node the observer send the snapshot msg
            if(len(self.in_queue)!=0):
                if(not self.in_queue['0'].empty()):
                    if(first_snap):
                        first_snap = False
                        save_state = self.balance
                        msg = self.in_queue['0'].get()
                        channel_state={}
                        for other_node_id in self.out_queue:
                            if other_node_id != '0':
                                self.out_queue[other_node_id].put(msg)
                                channel_state[other_node_id] = []
                        #self.snapshot_event.clear()
