import threading
import socket
import time
import sys

TCP_IP = socket.gethostname()

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

def run_reduce(line):
    commandline = line.split()
    inputfile1 = commandline[1]
    inputfile2 = commandline[2]

    file1 = open(inputfile1, 'r')
    file2 = open(inputfile2, 'r')
    
    string_of_file1 = file1.read()
    string_of_file2 = file2.read()

    string_of_file1 = string_of_file1.replace("<","")
    string_of_file1 = string_of_file1.replace(">","")
    string_of_file2 = string_of_file2.replace("<","")
    string_of_file2 = string_of_file2.replace(">","")

    file1_list = string_of_file1.split()
    file2_list = string_of_file2.split()

    file1_keys = []
    file1_values = []
    file2_keys = []
    file2_values = []
    
    for i in range(0, len(file1_list)):
        string = file1_list[i]
        index = file1_list[i].find(",")
        file1_keys.append(string[:index])
        file1_values.append(int(string[index+1:]))

    for i in range(0, len(file2_list)):
        string = file2_list[i]
        index = file2_list[i].find(",")
        file2_keys.append(string[:index])
        file2_values.append(int(string[index+1:]))

    reduced_keys = []
    reduced_values = []

    for i in range(0, len(file1_keys)):
        reduced_keys.append(file1_keys[i])
        reduced_values.append(file1_values[i])

    for i in range(0, len(file2_keys)):
        if(file2_keys[i] in reduced_keys):
            index = reduced_keys.index(file2_keys[i])
            reduced_values[index] = reduced_values[index] + file2_values[i]
        else:
            reduced_keys.append(file2_keys[i])
            reduced_values.append(file2_values[i])

    input_name = inputfile1
    input_name = input_name.replace("_I_1.txt","")
            
    output = str(input_name)+"_reduced.txt"
    
    output_file = open(output, "w")
    output_file.write(str(dictToString(reduced_keys, reduced_values))+"\n")
            
def run_cli():
    command = ""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, 5000))
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.listen(1)
    while(command != "exit"):    
        command = raw_input("Enter a command: ");
        list = command.split()
        if(list[0] == "map"):
            file = open(list[1], "r")
            chars = file.read()
            num_chars = len(chars)
            midpoint = num_chars/2
            while not(str(chars[midpoint]).isspace()):
                midpoint = midpoint + 1

            sock1 = socket.socket()
            sock1.connect((TCP_IP, 5001))
            msg1 = ""
            msg1 = msg1 + "map " + list[1] + " " + str(0) + " " + str(midpoint)
            sock1.send(msg1)
            sock1.close

            sock2 = socket.socket()
            sock2.connect((TCP_IP, 5002))
            msg2 = ""
            msg2 = msg2 + "map " + list[1] + " " + str(midpoint+1) + " " + str(num_chars-midpoint)
            sock2.send(msg2)
            sock2.close
            
        elif(list[0] == "reduce"):
            sock = socket.socket()
            sock.connect((TCP_IP, 5003))
            sock.send(command)
            sock.close
            
        elif(list[0] == "replicate"):
            print "replicate"
        elif(list[0] == "stop"):
            print "stop prm"
        elif(list[0] == "resume"):
            print "resume prm"
        elif(list[0] == "exit"):
            pass
        else:
            print "command not found";

def run_mapper1():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, 5001))
    s.listen(1)
    while True:
        c, addr = s.accept()
        line = c.recv(1024)
        run_map(line, 1)
        c.close()
        
def run_mapper2():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, 5002))
    s.listen(1)
    while True:
        c, addr = s.accept()
        line = c.recv(1024)
        run_map(line, 2)
        c.close()
        
def run_reducer():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, 5003))
    s.listen(1)
    while True:
        c, addr = s.accept()
        line = c.recv(1024)
        run_reduce(line)
        c.close()
    
def run_prm():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, 5004))
    s.listen(1)
    while True:
        c, addr = s.accept()
        c.close()

def blah():
    print "stub"

mapper1 = threading.Thread(target = run_mapper1)
mapper2 = threading.Thread(target = run_mapper2)
reducer = threading.Thread(target = run_reducer)
cli = threading.Thread(target = run_cli)
prm = threading.Thread(target = blah)

mapper1.start()
mapper2.start()
reducer.start()
cli.start()
prm.start()
