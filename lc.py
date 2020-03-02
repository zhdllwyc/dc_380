


def landy_champort_send(send_node):
    current_state = started_node.balance
    recorded_channel = {}
    for node_ID in started_node.out_queue:
        if(node_ID!=0):
            curr_out_queue = started_node.out_queue[node_ID]
            curr_out_queue.put("TakeSnapshot")
    for node_ID in started_node.in_queue:
        if(node_ID!=0):
            recorded_channel[node_ID] = []

#first time receive the message
def landy_champort_receive(lc_state):
    
