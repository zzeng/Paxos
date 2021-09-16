import socket
import string
import sys

def dictToString(keys, values):
    string = ""
    for i in range(0, len(keys)):
        string = string + "<"+keys[i]+","+str(values[i])+"> "
    return string
    
def run_map(line, mapper_id):
    commandline = line.split()
    input_file = commandline[1]
    offset = int(commandline[2])
    size = int(commandline[3])

    file = open(input_file, 'r')
    file.seek(offset)
    cropped_file = file.read(size)
    list_of_words = cropped_file.split()
    keys = []
    values = []
    for i in range(0,len(list_of_words)):
        if(list_of_words[i] in keys):
            values[keys.index(list_of_words[i])] += 1
        else:
            keys.append(list_of_words[i])
            values.append(1)
            
    output = str(input_file).replace(".txt","")+"_I_"+str(mapper_id)+".txt"
    
    output_file = open(output, "w")
    output_file.write(str(dictToString(keys, values))+"\n")

id = int(sys.argv[1])
setup_file = open(sys.argv[2], 'r')
setup = setup_file.readlines()
#TCP_IP = setup[id]
TCP_IP = socket.gethostname()

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((TCP_IP, 5001))
sock.listen(1)
while True:
    c, addr = sock.accept()
    line = c.recv(1024)
    run_map(line, 1)
    c.close()
    
