#!/usr/bin/python           # This is client.py file

import socket               # Import socket module

s = socket.socket()         # Create a socket object
host = socket.gethostname() # Get local machine name
port = 5000                 # Reserve a port for your service.

s.connect((host, port))
print s.recv(1024)
#print "peer: ",s.getpeername()
#print s.getpeername()[1]
print "sock: ",s.getsockname()
print s.getsockname()[1]
s.close                     # Close the socket when done
