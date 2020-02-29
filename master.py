import sys
import multiprocessing 
  
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
  
def argument_parsing(current_command_list):
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

    for command in command_list:
        argument_parsing(command)
	       







