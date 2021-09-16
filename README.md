------------------------
     Summary
------------------------
Maps and reduces files based on word count. Replicates logs of map/reduce
results to save the state of any performed computations through the use of 
Multi-Paxos. This programming model requires a cloud computing environment
such as Eucalyptus to run.


------------------------
     How to Run
------------------------
1. cd into directory
2. scp –i {keyname} {keyname} ubuntu@{public IP}:~/
3. ssh to the public machine and from there, ssh to the private machine (there should be 3 machines total)
4. on each private machine, 'python prm.py # filename.txt'
5. on 3 new private machines using the same addresses, 'python cli.py # filename.txt'
6. type in commands 


------------------------
     Commands
------------------------
/map 'filename.txt'

/reduce 'filename1.txt' 'filename2.txt'

/replicate 'filename.txt'

/stop

/resume

/total 'pos1' 'pos2'

/print

/merge 'pos1' 'pos2'