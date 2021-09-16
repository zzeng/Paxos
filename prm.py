import socket
import time
import sys

class file_object:
    def __init__(self, file_name, keys, values):
        self.file_name = file_name
        self.keys = keys
        self.values = values

    def __str__(self):
        if (self.file_name == ""):
            return "None"
        else:
            string = ""
            string = string + self.file_name + " "
            for i in range(len(self.keys)):
                string = string + "<"+self.keys[i]+","+str(self.values[i])+"> "
            return string

    def updateUsingString(self, string):
        if(string == "None"):
            self.file_name = ""
            self.keys = None
            self.values = None
        else:
            index = string.find(" ")
            self.file_name = string[:index]
            dict = string[index+1:]
            dict = dict.replace("<","")
            dict = dict.replace(">","")
            dict_list = dict.split()
            for i in range(len(dict_list)):
                pairs = dict_list[i]
                index2 = dict_list[i].find(",")
                self.keys.append(pairs[:index2])
                self.values.append(int(pairs[index2+1:]))

    def printFilename(self):
        print self.file_name

class log_object:
    def __init__(self, list):
            self.list = list

    def __str__(self):
        string = ""
        for i in self.list:
            string = string + str(i) + "\n"
        return string

    def updateUsingString(self, string):
        lines = string.splitlines()
        self.list = []
        for i in lines:
            obj = file_object("",[],[])
            obj.updateUsingString(i)
            (self.list).append(obj)

    def append(self, obj):
        (self.list).append(obj)

    def printFilenames(self):
        for i in self.list:
            i.printFilename()

class Paxos:
    def __init__(self, ballotNum, acceptNum, acceptVal, initialVal, list_of_ack, list_of_accept):
        self.ballotNum = ballotNum
        self.acceptNum = acceptNum
        self.acceptVal = acceptVal
        self.initialVal = initialVal
        self.list_of_ack = list_of_ack
        self.list_of_accept = list_of_accept

    def __str__(self):
        string = ""
        string = string + ballotNumToString(self.ballotNum) + ballotNumToString(self.acceptNum) + str(self.acceptVal) + str(self.initialVal)  + str(self.list_of_ack) + str(self.list_of_accept) + str(self.list_of_yes)
        return string

    def getBallotNum(self):
        return self.ballotNum

    def setBallotNum(self, ballot):
        self.ballotNum[0] = ballot[0]
        self.ballotNum[1] = ballot[1]

    def getAcceptNum(self):
        return self.acceptNum

    def setAcceptNum(self, ballot):
        self.acceptNum[0] = ballot[0]
        self.acceptNum[1] = ballot[1]

    def getAcceptVal(self):
        return self.acceptVal

    def setAcceptVal(self, acceptVal):
        self.acceptVal = acceptVal

    def getInitialVal(self):
        return self.initialVal

    def setInitialVal(self, initialVal):
        self.initialVal = initialVal

    def getListOfAck(self):
        return self.list_of_ack

    def appendAck(self, ack):
        (self.list_of_ack).append(ack)

    def getListOfAccept(self):
        return self.list_of_accept

    def appendAccept(self, accept):
        (self.list_of_accept).append(accept)
        
#get the ips of my machine and other machins
id = int(sys.argv[1])
setup_file = open(sys.argv[2], 'r')
setup = setup_file.readlines()
outgoing_ids = []
for i in range(0, int(setup[0])):
    outgoing_ids.append(i+1)

outgoing_ids.remove(id)

TCP_IP = setup[id]
#TCP_IP = socket.gethostname()
port = 5004
#port = setup[id]
num_of_ips = len(setup) - 1

#create a list of logs
#list_of_logs = []
#for i in range(num_of_ips):
#    j = []
#    log = log_object(j)
#    list_of_logs.append(log)
log = log_object([])

prm_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
prm_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
prm_sock.bind((TCP_IP, int(port)))
prm_sock.listen(3)

time.sleep(3)

#creates a list of sockets for receiving and sending
sock_in = []
sock_out = []

index = 0
running = True
list_of_paxos = []
available = True
queue = []

for i in range(0, len(outgoing_ids)):
    sock_out.append(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    while True:
        try:
            #sock_out[i].connect((socket.gethostname(), int(setup[outgoing_ids[i]])))
            sock_out[i].connect((setup[outgoing_ids[i]],5004))
            break
        except Exception:
            pass

for i in range(0, num_of_ips):
    c, addr = prm_sock.accept()
    c.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    c.setblocking(0)
    sock_in.append(c)

def ballotNumToString(ballotNum):
    string = ""
    string = string + "<"+str(ballotNum[0])+","+str(ballotNum[1])+">"
    return string

# check if num1 is greater than num2
def ballotGreaterThan(num1, num2):
    if (num1[0] > num2[0]):
        return True
    elif (num1[0] == num2[0]):
        if(num1[1] >= num2[1]):
            return True
        else:
            return False
    else:
        return False

def toBallot(string):
    string = string.replace("<","")
    string = string.replace(">","")
    tup = string.split(",")
    ballot = [0,0]
    ballot[0] = int(tup[0])
    ballot[1] = int(tup[1])
    return ballot

def getSockOut(addr):
    ip = addr[0]
    for i in sock_out:
        if (i.getpeername()[0] == ip):
            return i

def check_messages():
    for i in sock_in:
        try:
            received_msg = i.recv(1024)
            address = i.getpeername()
            lines = received_msg.splitlines()
            for i in lines:
                #run_command(i)
                run_command(i, address)
        except Exception:
            pass

def dictToString(keys, values):
    string = ""
    for i in range(0, len(keys)):
        string = string + "<"+keys[i]+","+str(values[i])+"> "
    return string

def run_command(command, address):
#def run_command(command):
    global running
    global index
    global list_of_paxos
    global available
    global log
    global queue
    received_msg = command
    msg = received_msg.split()
    if(msg[0] == "stop"):
        running = False
    elif(msg[0] == "resume"):
        running = True
        #need to block until finish catchup
        for i in sock_out:
            i.send("query "+command + "\n")
    elif(msg[0] == "total"):
        index = received_msg.find(" ")
        string_of_pos = received_msg[index+1:]
        list_of_pos = string_of_pos.split()
        total = 0
        for i in list_of_pos:
            log_i = log.list[int(i)-1]
            for j in log_i.values:
                total = total + j
        print total
    elif(msg[0] == "merge"):
        pos1 = int(msg[1])
        pos2 = int(msg[2])
        string1 = str(log.list[pos1-1])
        string2 = str(log.list[pos2-1])
        k = string1.find(" ")
        string1 = string1[k+1:]
        l = string2.find(" ")
        string2 = string2[l+1:]
        string1 = string1.replace("<","")
        string1 = string1.replace(">","")
        string2 = string2.replace("<","")
        string2 = string2.replace(">","")
        list1 = string1.split()
        list2 = string2.split()
        file1_keys = []
        file1_values = []
        file2_keys = []
        file2_values = []   
        for i in range(0, len(list1)):
            string = list1[i]
            i = list1[i].find(",")
            file1_keys.append(string[:i])
            file1_values.append(int(string[i+1:]))
        for i in range(0, len(list2)):
            string = list2[i]
            i = list2[i].find(",")
            file2_keys.append(string[:i])
            file2_values.append(int(string[i+1:]))
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
        print str(dictToString(reduced_keys,reduced_values))
    elif(msg[0] == "print"):
        log.printFilenames()
    else:
        if(running):
            if(msg[0] == "replicate"):
                currentIndex = index
                if(available):
                    for i in sock_out:
                        i.send("initiate " + str(currentIndex) + " " + received_msg + "\n")
                else:
                    queue.append(received_msg)
            elif(msg[0] == "initiate"):
                currentIndex = int(msg[1])
                k = received_msg.find(" ")
                command = received_msg[k+1:]
                k = command.find(" ")
                command = command[k+1:]
                if(currentIndex == index):
                    #find a way to send message back
                    #sock_out[0].send("yes "+command)
                    getSockOut(address).send("yes "+command + "\n")
                    list_of_paxos.append(Paxos([0,0],[0,0],None,None,[],[]))
                    available = False
            elif(msg[0] == "yes"):
                currentIndex = index
                if not (currentIndex == len(list_of_paxos)):
                    pass
                else:
                    k = received_msg.find(" ")
                    command = received_msg[k+1:]
                    msg2 = command.split()
                    list_of_paxos.append(Paxos([0,0],[0,0],None,None,[],[]))
                    available = False
                    file_name = msg2[1]
                    file = open(file_name, "r")
                    content = file.read()
                    string = file_name + " " + content
                    initialVal = file_object("",[],[])
                    initialVal.updateUsingString(string)
                    list_of_paxos[currentIndex].setInitialVal(initialVal)
        
                    ballot = list_of_paxos[currentIndex].getBallotNum()
                    ballot[0] = ballot[0] + 1
                    ballot[1] = id
                    list_of_paxos[currentIndex].setBallotNum(ballot)
                    for i in sock_out:
                        i.send("prepare " + str(index) + " " + ballotNumToString(list_of_paxos[currentIndex].getBallotNum()) + "\n")
                    
            elif(msg[0] == "query"):
                currentIndex = index
                #sock_out[0].send("results " + str(currentIndex) + str(log))
                getSockOut(address).send("results " + str(currentIndex) + " " + str(log) + "\n")
            elif(msg[0] == "results"):
                possibleIndex = int(msg[1])
                currentIndex = index
                if(currentIndex < possibleIndex):
                    index = possibleIndex
                    k = received_msg.find(" ")
                    string = received_msg[k+1:]
                    k = string.find(" ")
                    string = string[k+1:]
                    log.updateUsingString(string)
                #blocking prm until this is ran need to figure out how many results needed
            elif(msg[0] == "prepare"):
                currentIndex = int(msg[1])
                receivedNum = [0,0]
                msg[2] = msg[2].replace("<", "")
                msg[2] = msg[2].replace(">", "")
                ballot = msg[2].split(",")
                receivedNum[0] = int(ballot[0])
                receivedNum[1] = int(ballot[1])
                if (ballotGreaterThan(receivedNum, list_of_paxos[currentIndex].getBallotNum())):
                    list_of_paxos[currentIndex].setBallotNum(receivedNum)
                    
                    if not (id == receivedNum[1]):
                        if(list_of_paxos[currentIndex].getAcceptVal == None):
                            #sock_out[0].send("ack "+ str(currentIndex) + " " + ballotNumToString(list_of_paxos[currentIndex].getBallotNum())+" "+ballotNumToString(list_of_paxos[currentIndex].getAcceptNum())+" "+"None" + "\n")
                            getSockOut(address).send("ack "+ str(currentIndex) + " " + ballotNumToString(list_of_paxos[currentIndex].getBallotNum())+" "+ballotNumToString(list_of_paxos[currentIndex].getAcceptNum())+" "+"None" + "\n")
                        else:
                            #sock_out[0].send("ack "+ str(currentIndex) + " " + ballotNumToString(list_of_paxos[currentIndex].getBallotNum())+" "+ballotNumToString(list_of_paxos[currentIndex].getAcceptNum())+" "+str(list_of_paxos[currentIndex].getAcceptVal()) + "\n")
                            getSockOut(address).send("ack "+ str(currentIndex) + " " + ballotNumToString(list_of_paxos[currentIndex].getBallotNum())+" "+ballotNumToString(list_of_paxos[currentIndex].getAcceptNum())+" "+str(list_of_paxos[currentIndex].getAcceptVal()) + "\n")
            elif(msg[0] == "ack"):
                currentIndex = int(msg[1])
                if(received_msg in list_of_paxos[currentIndex].getListOfAck()):
                    pass
                else:
                    list_of_paxos[currentIndex].appendAck(received_msg)
                    receivedBallot = toBallot(msg[2])
                    receivedNum = toBallot(msg[3])
                    k = received_msg.find(" ")
                    stringOfReceivedVal = received_msg[k+1:]
                    k = stringOfReceivedVal.find(" ")
                    stringOfReceivedVal = stringOfReceivedVal[k+1:]
                    k = stringOfReceivedVal.find(" ")
                    stringOfReceivedVal = stringOfReceivedVal[k+1:]
                    k = stringOfReceivedVal.find(" ")
                    stringOfReceivedVal = stringOfReceivedVal[k+1:]
                    if(stringOfReceivedVal == "None"):
                        receivedVal = None
                    else:
                        receivedVal = file_object("",[],[])
                        receivedVal.updateUsingString(stringOfReceivedVal)
                    if(list_of_paxos[currentIndex].getAcceptVal() == None and receivedVal == None):
                        list_of_paxos[currentIndex].setAcceptVal(list_of_paxos[currentIndex].getInitialVal())
                        list_of_paxos[currentIndex].setAcceptNum(list_of_paxos[currentIndex].getBallotNum())
                    else:
                        if(ballotGreaterThan(receivedNum, list_of_paxos[currentIndex].getAcceptNum())):
                            list_of_paxos[currentIndex].setAcceptVal(receivedVal)
                            list_of_paxos[currentIndex].setAcceptNum(list_of_paxos[currentIndex].getBallotNum())
                        else:
                            list_of_paxos[currentIndex].setAcceptVal(list_of_paxos[currentIndex].getInitialVal())
                            list_of_paxos[currentIndex].setAcceptNum(list_of_paxos[currentIndex].getBallotNum())
                    for i in sock_out:
                        i.send("accept "+ str(currentIndex) + " " + ballotNumToString(list_of_paxos[currentIndex].getBallotNum()) + " " + str(list_of_paxos[currentIndex].getAcceptVal()) + "\n")
            elif(msg[0] == "accept"):
                currentIndex = int(msg[1])
                paxos = list_of_paxos[currentIndex]
                if(received_msg in paxos.getListOfAccept()):
                    pass
                else:
                    paxos.appendAccept(received_msg)
                    receivedBallot = toBallot(msg[2])
                    k = received_msg.find(" ")
                    stringOfReceivedVal = received_msg[k+1:]
                    k = stringOfReceivedVal.find(" ")
                    stringOfReceivedVal = stringOfReceivedVal[k+1:]
                    k = stringOfReceivedVal.find(" ")
                    stringOfReceivedVal = stringOfReceivedVal[k+1:]
                    receivedVal = file_object("",[],[])
                    receivedVal.updateUsingString(stringOfReceivedVal)
                    if(ballotGreaterThan(receivedBallot, paxos.getBallotNum())):
                        paxos.setAcceptNum(receivedBallot)
                        paxos.setAcceptVal(receivedVal)
                        for i in sock_out:
                            i.send(received_msg + "\n")
                        log.append(paxos.getAcceptVal())
                        index = index + 1
                        available = True
                        if not (len(queue) == 0):
                            command = queue[0]
                            del queue[0]
                            currentIndex = index
                            if(available):
                                for i in sock_out:
                                    i.send("initiate " + str(currentIndex) + " " + command + "\n")
                            else:
                                queue.append(received_msg)
                                
while True:
    time.sleep(2.5)
    check_messages()
