import socket

s = socket.socket()
s.bind((socket.gethostname(),5000))

s.listen(1)
while True:
    c, addr = s.accept()
    c.setblocking(0)
    try:
        print c.recv(1024)
    except Exception:
        pass
    command = raw_input("Enter command: ")
    c.sendall(command)
            
