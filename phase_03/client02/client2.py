import sys
import socket

def Main():
    
    host = 'server_2' # localhost # alias of server 02 in docker network
    port =  5030
    
    name_of_subscriber = str(sys.argv[1])
    print("The Subscriber Crypto is :",name_of_subscriber)
    
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.connect((host,port))  # Connect to server
    
    signal = True
    
    while True:
       
        if signal:
            s.send(name_of_subscriber.encode())
            signal = False
        
        data = s.recv(2048).decode()
        print(data)


if __name__ == '__main__':
    Main() 
