#!/usr/bin/python           # This is server.py file

import socket               # Import socket module

s = socket.socket()         # Create a socket object
host = socket.gethostname() # Get local machine name
port = 5000                 # Reserve a port for your service.
s.bind((host, port))        # Bind to the port

s.listen(5)                 # Now wait for client connection.
while True:
   c, addr = s.accept()     # Establish connection with client.
   c.send('Thank you for connecting')
   print c.getpeername()
   print c.getpeername()[1]
   #print c.getsockname()
   #print c.getsockname()[1]
   c.close()                # Close the connection
