import socket
import sys
import random

from _thread import *
from threading import Timer

# gossip protocol variables
# nodes = int(sys.argv[1])
# times = int(sys.argv[2])
# message = 'Hello!'


def percentage(part, whole):
    return 100 * float(part)/float(whole)


class GossipNode:
    available_nodes = []
    infected_nodes = []

    def __init__(self, number, topic=None, message=None):
        DEFAULT_MESSAGE = 1
        self.number = number
        self.message = message if message is not None else DEFAULT_MESSAGE
        self.topic = topic if topic is not None else "DEFAULT_TOPIC"
        
    def send_message(self, message, topic):
        print(self.available_nodes)
        selected_nodes = []
        # selected_nodes = random.choices(self.available_nodes, k=5)
        for i in self.available_nodes:
            if i.topic == topic:
                selected_nodes.append(i)
        for node in selected_nodes:
            node.recieve_message(message)

    def recieve_message(self, message):
        while self.message != message:
            self.message = message
            GossipNode.infected_nodes.append(self.number)
            self.send_message(message)

# Topic based pub-sub -- Multithreading and Socket is needed for implementation

# Global variables

list_of_clients = []

list_of_topics = ['weather','politics','sports','SCU','COEN317']

topics = ['SCU','COEN317', "Bitcoin"] # server1's responsibility is generate events of these topics

subscriptions = {}

events = { 'SCU' : ['Summer registration is open!', 'There will be a seminar on pub-sub', 'SCU-hackathon is tomorrow'],
    'COEN317' : ['Implement MAP with weather info', 'Learn Distributed Systems', 'Blockchain is the new trend'],
    'Bitcoin' : ['Implement BCBBCBCBC with weather info', 'Learn Web3', 'Metamask']
}

# { subscriberName : [msg1, msg2,, ] , ... }
generated_events = dict()

signal = dict()

# Handle any client's connection

def clientThread(connection, data):
    while True:
        signal[data] = 0
        subscription(data) # Generate subscription for the connected subscriber
        subscribe_info = 'Your subscriptions are : ' + str(data.topic)
        connection.send(subscribe_info.encode())
        
        while True:
            if signal[data]==1:
                notify(connection,data)

    connection.close()



# Handle other server's connection

# Send to other servers
def server_thread_sender(connection, data):
    while True:
        signal[data] = 0
        subscriptions[data] = topics  # Other server's  are subscribed to the all topics of this server
        subscribe_info = 'Your subscriptions are : ' + str(subscriptions[data])
        connection.send(subscribe_info.encode())
        
        while True:
            if signal[data]==1:
                notify(connection,data)
    connection.close()


# Receive from other servers
def server_thread_receiver(connection, data):
    while True:
        serverData = connection.recv(2048).decode()
        m = serverData.split('-')
        if len(m)==2:
            topic = m[0]
            event = m[1]
            publisher(topic,event,0)
    connection.close()


# Send to master server
def master_thread_sender(ss):
    while True:
        signal['master'] = 0
        subscriptions['master'] = topics  # Other server's  are subscribed to the all topics of this server
        subscribe_info = 'Your subscriptions are : ' + str(subscriptions['master'])
        ss.send(subscribe_info.encode())
        
        while True:
            if signal['master']==1:
                notify(ss,'master')
    ss.close()

# Receive from master server
def master_thread_receiver(ss):
    while True:
        serverData = ss.recv(2048).decode()
        if serverData:
            print("Received from MASTER :",serverData)
            p = serverData.split('-')
            if len(p)==2:
                topic = p[0]
                event = p[1]
                publisher(topic,event,0)
    connection.close()



## SUBSCRIBE()


def subscription(name):
    # subscriptions[name] = generate_subscription_randomly()
    name.topic = generate_subscription_randomly()


def generate_subscription_randomly():
    subscribedTopicsList = random.sample(list_of_topics,random.choice(list(range(1,len(list_of_topics)+1))))
    return subscribedTopicsList



## PUBLISH() and ADVERTIZE()

def generate_event():
    
    topic = random.choice(topics)
    msgList = events[topic]
    event = msgList[random.choice(list(range(1,len(msgList))))]
    
    publisher(topic,event,1) # call publisher() for publishing the new event


def publisher(topic,event,indicator):
    
    event = topic + '-' + event  # Concatenate topic and event
    print(event)  # print the event in server console
    
    # publishing generated events to interested subscriber (subscribers + other servers)
    if indicator == 1:
        for name, topics in subscriptions.items() :
            if topic in topics:
                if name in generated_events.keys():
                    generated_events[name].append(event)
                else:
                    generated_events.setdefault(name, []).append(event)
                signal[name] = 1

    # publishing received events to interested subscriber (only subscribers)
    else:
        seed_node = 0
        for i in GossipNode.available_nodes:
            if i.topic == topic:
                seed_node = i
                break
        seed_node.send_message(event)

    t = Timer(random.choice(list(range(30,36))), generate_event)
    t.start()


def notify(connection,name):
    if name in generated_events.keys():
        for msg in generated_events[name]:
            msg = msg  # + str("\n")
            connection.send(msg.encode())
        del generated_events[name]
        signal[name] = 0



def Main():
    
    host = ""     # Server will accept connections on all available IPv4 interfaces
    port = 5030   # Port to listen on (non-privileged ports are > 1023)
    
    # API orders : socket() -> bind() -> listen() -> accept()
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Creating a socket object with TCP protocol. AF_INET is the Internet address family for IPv4.
    
    s.bind((host,port))  # Binding the socket object to a port
    
    
    print("Socket is bind to the port :", port)
    
    s.listen(5)  # Socket is now listening to the port for new connection with 'backlog' parameter value 5. It defines the length of queue for pending connections
    
    print("Socket is now listening for new connection ...")
    
    
    # generate_event() will be called in a new thread after 10 to 15 seconds
    t = Timer(random.choice(list(range(30,36))), generate_event)
    t.start()


    # connecting with master server

    master_host = 'server001' # localhost  # alias of server01 in docker network
    master_port =  5029
    
    serverName = str(sys.argv[1])
    
    ss = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    ss.connect((master_host,master_port))
    
    ss.send(serverName.encode())
    
    start_new_thread(master_thread_receiver, (ss,))
    start_new_thread(master_thread_sender, (ss,))
    
    
    # An infinity loop - server will be up for infinity and beyond
    while True:
        
        connection, addr = s.accept()  # Waiting for new connection to be accepted
        print('Connected to :', addr[0], ':', addr[1])
        print("Connection string is",connection)
        
        data = connection.recv(2048).decode()
        
        if data:
            print("Welcome ",data)
        
        l = data.split('-')
        
        if l[0]=='c':
            node = GossipNode(l[1])
            GossipNode.available_nodes.append(node)
            list_of_clients.append(l[1])
            # start_new_thread(clientThread, (connection,l[1]))
            start_new_thread(clientThread, (connection, node))
        if l[0]=='s':
            start_new_thread(server_thread_sender, (connection,l[1]))
            start_new_thread(server_thread_receiver, (connection,l[1]))
            

    s.close()


if __name__ == '__main__':
    Main()

