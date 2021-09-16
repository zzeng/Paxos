import socket
import time
import sys

id = int(sys.argv[1])
setup_file = open(sys.argv[2], 'r')
setup = setup_file.readlines()
#TCP_IP = setup[id]
TCP_IP = socket.gethostname()
port = 5004

prm_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
prm_sock.connect((socket.gethostname(), 5001))

command = ""
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((TCP_IP, port))
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.listen(1)

while(command != "exit"):
    command = raw_input("Enter a command: ");
    list = command.split()
    if(list[0] == "exit"):
        pass
    else:
        prm_sock.send(command)
