import socket
import string
import sys

def dictToString(keys, values):
    string = ""
    for i in range(0, len(keys)):
        string = string + "<"+keys[i]+","+str(values[i])+"> "
    return string

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
    input_name = input_name.replace("_I_1.txt", "")

    output = str(input_name) + "_reduced.txt"

    output_file = open(output, "w")
    output_file.write(str(dictToString(reduced_keys, reduced_values))+"\n")
    
id = int(sys.argv[1])
setup_file = open(sys.argv[2], 'r')
setup = setup_file.readlines()
#TCP_IP = setup[id]
TCP_IP = socket.gethostname()

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((TCP_IP, 5003))
sock.listen(1)
while True:
    c, addr = sock.accept()
    line = c.recv(1024)
    run_reduce(line)
    c.close()
