import sys
import time
import multiprocessing 
class Node:
    def __init__(self, node_type, node_process):
        self.node_type = node_type
        self.node_process = node_process
        
    
    def connect(self, All_process, All_node):
        All_Process.append(self.node_process)
       
        current_in_queue = multiprocessing.Queue()
        current_out_queue = multiprocessing.Queue()
        self.in_queue = current_in_queue
        self.out_queue = current_out_queue

        All_node.append(self)
        
        

def sender(conn, msgs): 
    """ 
    function to send messages to other end of pipe 
    """
    for msg in msgs: 
        conn.send(msg) 
        print("Sent the message: {}".format(msg)) 
    conn.close() 
  
def receiver(conn): 
    """ 
    function to print the messages received from other 
    end of pipe 
    """
    while 1: 
        msg = conn.recv() 
        if msg == "END": 
            break
        print("Received the message: {}".format(msg)) 

def Node_creation():


def KillAll(All_process):
    for process in All_process:
        process.terminate()

def StartMaster(All_Process):

    master_in_queue = multiprocessing.Queue()
    master_out_queue = multiprocessing.Queue()

    Master = multiprocessing.Process(target=Node_creation, args=('Master', current_queue)) 
    Observer = multiprocessing.Process(target=Node_creation, args=('Observer', current_queue)) 
  
    


def argument_parsing(current_command_list, All_Process, All_Node):
    if(current_command_list[0] == 'StartMaster'):
        print('StartMaster')

    elif(current_command_list[0] == 'CreateNode'):
        print('CreateNode')

    elif(current_command_list[0] == 'Send'):
        print('Send')

    elif(current_command_list[0] == 'Receive'):
        print('Receive')

    elif(current_command_list[0] == 'BeginSnapshot'):
        print('BeginSnapshot')

    elif(current_command_list[0] == 'KillAll'):
        print('KillAll')
        KillAll(All_process)

    elif(current_command_list[0] == 'ReceiveAll'):
        print('ReceiveAll')

    elif(current_command_list[0] == 'CollectState'):
        print('CollectState')

    elif(current_command_list[0] == 'PrintSnapshot'):
        print('PrintSnapshot')

if __name__ == "__main__": 
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

    All_Process = []
    All_Node = []
    for command in command_list:
        argument_parsing(command, All_Process, All_Node)
	       







