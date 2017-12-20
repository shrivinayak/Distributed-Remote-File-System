# Fault Tolerant Distributed User Space File System

- This is a userspace file system built using [python fusepy](https://github.com/terencehonles/fusepy) libraries.
- File system data is stored in memory for high throughput and written to disk in the background to provide persistancy
- The setup can be a single node machine or geographically separted systems. 

### The following libraries were used in building this file system.
- [Python Fusepy](https://github.com/terencehonles/fusepy) 
- [XMLRPC Server](https://docs.python.org/2/library/simplexmlrpcserver.html)
- [Pickle - For object serialization](https://docs.python.org/3/library/pickle.html)
- [Shelve - For persistant storage for dictionaries](https://docs.python.org/2/library/shelve.html)

### Instructions
1) Install Fusepy libraries
```sh
$ sudo apt-get install libfuse-dev git python-setuptools
```

2) Git clone the repo
```sh
$ git clone https://github.com/shrivinayak/Distributed-Remote-File-System.git
```
3) Create a mount point for the filesystem
```sh
$ mkdir mount
```
4) Start metaserver (this server holds file attribute information for the filesystem)
```sh
$ python metaserver.py 2222
```
> metaserver process is started with port number 2222, add & to the command to run in background

5) Start data-servers 
```sh
$ python dataserver.py 0 2222 3333 4444 5555 6666 &
$ python dataserver.py 1 2222 3333 4444 5555 6666 &
$ python dataserver.py 2 2222 3333 4444 5555 6666 &
$ python dataserver.py 3 2222 3333 4444 5555 6666 &
```


> Service is ready to be utilized

6) Mount the file system using client
```sh
$ python remoteFS.py mount 2222 3333 4444 5555 6666
```


### Note:
> Set BLOCK_SIZE in remoteFS.py and dataserver.py to configure block size. Every file is split into blocks of size BLOCK_SIZE and stored across all the data servers with the replication factor = REPLICATION_FACTOR

> A 32byte checksum is appended to every block of data stored which helps in detecting corruption. 

> Every data server writes to a local file called datastore_n.db where n is the data server number. Upon restart of the server data is loaded back into the memory from the file. 

