import socket
import time
import sys

id = int(sys.argv[1])
setup_file = open(sys.argv[2], 'r')
setup = setup_file.readlines()
TCP_IP = setup[id]
#TCP_IP = socket.gethostname()
port = 5000

command = ""
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((TCP_IP, port))
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.listen(1)

#mapper1Sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#mapper1Sock.connect((TCP_IP, 5001))
#mapper2Sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#mapper2Sock.connect((TCP_IP, 5002))
#reducerSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#reducerSock.connect((TCP_IP, 5003))
prmSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
prmSock.connect((TCP_IP, 5004))

while(command != "exit"):
    command = raw_input("Enter a command: ");
    list = command.split()
    #if(list[0] == "map"):
    #    file = open(list[1], "r")
    #    chars = file.read()
    #    num_chars = len(chars)
    #    midpoint = num_chars/2
    #    while not(str(chars[midpoint]).isspace()):
    #        midpoint = midpoint + 1
    #
    #    msg1 = ""
    #    msg1 = msg1 + "map " + list[1] + " " + str(0) + " " + str(midpoint)
    #    mapper1Sock.send(msg1)
    #    msg2 = ""
    #    msg2 = msg2 + "map " + list[1] + " " + str(midpoint + 1) + " " + str(num_chars-midpoint)
    #    mapper2Sock.send(msg2)
    #elif(list[0] == "reduce"):
    #    reducerSock.send(command)
    if(list[0] == "replicate"):
    #elif(list[0] == "replicate"):
        prmSock.send(command)
    elif(list[0] == "stop"):
        prmSock.send(command)
    elif(list[0] == "resume"):
        prmSock.send(command)
    elif(list[0] == "total"):
        prmSock.send(command)
    elif(list[0] == "print"):
        prmSock.send(command)
    elif(list[0] == "merge"):
        prmSock.send(command)
    elif(list[0] == "exit"):
    #    mapper1Sock.close()
    #    mapper2Sock.close()
    #    reducerSock.close()
        prmSock.close()
        break
    else:
        print "command not found"
