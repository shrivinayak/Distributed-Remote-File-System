#Shrivinayak_Bhat
#University_of_Florida


#This is the client code for distributed remote file system.

# Usage: python remoteFS.py <mountpoint> <metaserver_port> <dataserver_port>*n
# Eg: python remoteFS.py mount 2222 3333 4444 5555 6666


from __future__ import  absolute_import, division
import logging
from collections import defaultdict
from errno import ENOENT,ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time,sleep
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import os
import hashlib 


#for rpc
from xmlrpclib import ServerProxy, Binary
try:
   import cPickle as pickle
except:
   import pickle

#Cpickle is a faster conversion library


BLOCK_SIZE = 8 # also mentioned in dataserver.py
REPLICATION_FACTOR = 3
LOCK_WAIT_TIME = 0.0001


if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):

     # for every method such as self.symlink , the equivalent remote procedure is self.rc.metaServer.symlink & self.rc.metaServer.symlink



    def __init__(self,mountpoint,rc):
	self.rc = rc
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
	self.rc.acquire()
	self.rc.metaServer.put(self.rc.serialize('/'),self.rc.serialize(dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,st_mtime=now, st_atime=now, st_nlink=2)))
	self.rc.release()

	# this method splits the data into blocks and puts them in n data servers
    def distributedPut(self,path):
	self.rc.acquire()
	x = self.rc.deserialize(self.rc.metaServer.getHash(self.rc.serialize(path)))
	if not x:
		x = hash(path)
		self.rc.metaServer.putHash(self.rc.serialize(path),self.rc.serialize(x))

	self.rc.release()

        N = self.rc.n # No of data servers
        dblocks = []
        for i in range(N):
            dblocks.append([])

        dserver = x
        for blk in self.data[path]:
                for j in range(REPLICATION_FACTOR):
                        dblocks[(dserver+j+1)%N].append(blk)
                dserver+=1


        for i in range(N):
		for j in range(len(dblocks[i])):
			dblocks[i][j] += hashlib.md5(dblocks[i][j]).hexdigest()
		while 1:
			try:	
				while 1:
					try:
						self.rc.acquire()
                				self.rc.dataServer[i].putData(self.rc.serialize(path),self.rc.serialize(dblocks[i]))
						self.rc.release()
						break
					except:	
						self.rc.release()
						sleep(LOCK_WAIT_TIME)
						pass
				break
			except Exception as e:
				print e,'unable to put data into data server ',i
				sleep(LOCK_WAIT_TIME)



#helper method for distributedGet method
    def pointer(self,n,x,j,dblocks,i):
	x1 = (x+n+j+1)%self.rc.n
   	dblocks[x1].append(i)
	return x1,len(dblocks[x1])-1

# This method gets data from all data server and puts it together
    def distributedGet(self,path):
	data = []
	self.rc.acquire()
	st_size = self.rc.deserialize(self.rc.metaServer.getattr(self.rc.serialize(path)))['st_size']
	self.rc.release()

	# calculate k = number of blocks
	if st_size == 0:
		return ''
	k = int(st_size/BLOCK_SIZE)
	if st_size%BLOCK_SIZE :
		k+=1
	self.rc.acquire()
        hsh = self.rc.deserialize(self.rc.metaServer.getHash(self.rc.serialize(path)))
	self.rc.release()

        if not hsh:
                hsh = hash(path)
		self.rc.acquire()
                self.rc.metaServer.putHash(self.rc.serialize(path),self.rc.serialize(hsh))
		self.rc.release()
 

	dblocks = []
	for e in range(self.rc.n):
		dblocks.append([])
	data = []
	#get k blocks
	n = 0
	for i in range(k):	
		corrupt = []
		blk = False
		for j in range(REPLICATION_FACTOR):
			x,y = self.pointer(n,hsh,j,dblocks,i)
			try:
				self.rc.acquire()
				temp = self.rc.deserialize(self.rc.dataServer[x].getBlock(self.rc.serialize(path),self.rc.serialize(y)))
				self.rc.release()
				if temp[-32:] == hashlib.md5(temp[:len(temp)-32]).hexdigest():
					if not blk:
						blk = temp					
				else:
					corrupt.append((x,y))	

			except Exception as e:
				self.rc.release()
				print e		
		if not blk:
			print 'unable to recover blk ('+str(x)+','+str(y)+') of path: '+path
			break
		for tup in corrupt:
			print '\n\n> Data corruption encountered'
			print 'sent block copy of ',i,' to server ',tup[0],' for recovery.\n\n'

			self.rc.acquire()
			try:
				self.rc.dataServer[tup[0]].putBlock(self.rc.serialize(path),self.rc.serialize(blk),self.rc.serialize(tup[1]) ) 

			except:
				pass

			self.rc.release()

		data.append(blk[:len(blk)-32])
		n+=1
	print 'received data: ',data

	return data

    def chmod(self, path, mode):
	self.rc.acquire()
	temp = self.rc.metaServer.chmod(self.rc.serialize(path),self.rc.serialize(mode))
	self.rc.release()
	
	return self.rc.deserialize(temp)

    def chown(self, path, uid, gid):
	self.rc.acquire()
	self.rc.metaServer.chown(self.rc.serialize(path),self.rc.serialize(uid),self.rc.serialize(gid))
	self.rc.release()


    def create(self, path, mode):
	self.rc.acquire()
        x = self.rc.deserialize(self.rc.metaServer.getHash(self.rc.serialize(path)))
        if not x:
                self.rc.metaServer.putHash(self.rc.serialize(path),self.rc.serialize(hash(path)))

        self.rc.metaServer.create(self.rc.serialize(path),self.rc.serialize(mode),self.rc.serialize(time()))
	self.rc.release()


	self.fd+=1
	return self.fd

    def getattr(self, path, fh=None):

	self.rc.acquire()
	temp = self.rc.deserialize(self.rc.metaServer.matchKey(self.rc.serialize(path)))
	if temp == 0:
		self.rc.release()
		raise FuseOSError(ENOENT)	
	else:
		temp= self.rc.deserialize(self.rc.metaServer.getattr(self.rc.serialize(path)))
		self.rc.release()
		return temp

    def getxattr(self, path, name, position=0):
	self.rc.acquire()
        temp = self.rc.deserialize(self.rc.metaServer.getxattr(self.rc.serialize(path),self.rc.serialize(name)))
	self.rc.release()
        if str(temp)!='error':
            return temp
        else:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
	self.rc.acquire()
        temp = self.rc.deserialize(self.rc.metaServer.listxattr(self.rc.serialize(path)))
	self.rc.release()
	return temp

    def parent(self, path):
	temp='/'.join(path.split('/')[:-1])
	if temp=='':
		return '/'
	else:
		return temp

    def mkdir(self, path, mode):
	
	self.rc.acquire()
	self.rc.metaServer.mkdir(self.rc.serialize(path),self.rc.serialize(mode),self.rc.serialize(time()))
	self.rc.release()


    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
	temp = self.distributedGet(path)

	if not temp:
            return ''
	
	temp1 = temp[int(offset/BLOCK_SIZE)+1:int((offset+size)/BLOCK_SIZE)]
    	temp = list(temp[int(offset/BLOCK_SIZE)][offset%BLOCK_SIZE:])+temp1+temp[:((offset+size)%BLOCK_SIZE)+1]
	return ''.join(temp)

    def readdir(self, path, fh):	
	self.rc.acquire()
        temp= self.rc.deserialize(self.rc.metaServer.readdir(self.rc.serialize(path),self.rc.serialize(fh)))
	self.rc.release()
	return temp

    def readlink(self, path):
	return ''.join(self.distributedGet(path))

    def removexattr(self, path, name):
	self.rc.metaServer.removexattr(self.rc.serialize(path),self.rc.serialize(name))

    def rename(self, old, new):

	self.rc.acquire()
	self.rc.metaServer.rename(self.rc.serialize(old),self.rc.serialize(new))
	for i in range(self.rc.n):
		try:
			self.rc.dataServer[i].rename(self.rc.serialize(old),self.rc.serialize(new))
		except:
			pass

	
	keys = self.rc.deserialize(self.rc.metaServer.getHashKeys())
	for e in keys:
      		if old in e:
			if old == e[:len(old)]:
				hsh = self.rc.deserialize(self.rc.metaServer.getHash(self.rc.serialize(e)))
				self.rc.metaServer.removeHash(self.rc.serialize(e))
				self.rc.metaServer.putHash(self.rc.serialize(e.replace(old,new)),self.rc.serialize(hsh))
	self.rc.release()

    def rmdir(self, path):
	if len(self.readdir(path,0))>2:
		raise FuseOSError(ENOTEMPTY)	
	self.rc.acquire()	
	self.rc.metaServer.rmdir(self.rc.serialize(path))
	self.rc.metaServer.removeHash(self.rc.serialize(path))
	self.rc.release()
	
    def setxattr(self, path, name, value, options, position=0):

	self.rc.acquire()
        self.rc.metaServer.setxattr(self.rc.serialize(path),self.rc.serialize(name),self.rc.serialize(value))
	self.rc.release()
	
    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
	self.rc.acquire()

	self.rc.metaServer.symlink(self.rc.serialize(target),self.rc.serialize(source))
	hsh = self.rc.deserialize(self.rc.metaServer.getHash(self.rc.serialize(self.parent(target)+source)))

        self.rc.metaServer.putHash(self.rc.serialize(target),self.rc.serialize(hsh))
	
	self.rc.release()
	try:
		self.write(target,source,0,0)	
	except Exception as e:
		print e



    def truncate(self, path, length, fh=None):

	self.data[path] = self.distributedGet(path)
	self.rc.acquire()
	self.rc.metaServer.truncate(self.rc.serialize(path),self.rc.serialize(length))
	self.rc.release()
	
	st_size = self.rc.deserialize(self.rc.metaServer.getattr(self.rc.serialize(path)))['st_size']
	try:	
    		offset=length
    		i = int(offset / BLOCK_SIZE)
    		if offset%BLOCK_SIZE !=0:
      			lastblock = self.data[path][i][:int(offset%BLOCK_SIZE)]
      			self.data[path] = self.data[path][:i]
      			self.data[path].append(lastblock)
    		else:
      			self.data[path] = self.data[path][:i]
		self.distributedPut(path) # calling distributed put
        	data=self.data.pop(path)
	except:
        	data=self.data.pop(path)
		self.write(path,''.join(data)+'\x00'*(length-st_size),0,0)
		




    def unlink(self, path):
	self.rc.acquire()
	self.rc.metaServer.unlink(self.rc.serialize(path))
	for i in range(self.rc.n):
		while 1:
			try:
				self.rc.dataServer[i].unlink(self.rc.serialize(path))
				break
			except Exception as e:
				print e
	self.rc.metaServer.removeHash(self.rc.serialize(path))

	self.rc.release()


    def utimens(self, path, times=None):
	self.rc.acquire()

	if times==None:
		times=0
        self.rc.metaServer.utimens(self.rc.serialize(path),self.rc.serialize(times[0]),self.rc.serialize(time()))
	self.rc.release()


    def write(self, path, data, offset, fh):
	try:
		self.data[path] = self.distributedGet(path)
	except:
		self.data[path] = False
	if self.data[path]:
      		i = int(offset / BLOCK_SIZE)
      		if offset%BLOCK_SIZE !=0:
        		lastblock = self.data[path][i][:int(offset%BLOCK_SIZE)]
        		self.data[path] = self.data[path][:i]
        		self.data[path].append(lastblock)
      		else:
        		self.data[path] = self.data[path][:i]
    	else:
      		self.data[path] = []
      		i = 0
    	j=0

	while j<len(data):
      		if len(self.data[path]) == i:
        		self.data[path].append('')
      		if len(self.data[path][i]) == BLOCK_SIZE:
        		i+=1
      		else:
        		self.data[path][i]+=data[j]
        		j+=1
	
	
	self.distributedPut(path) # calling distributed put

	self.rc.acquire()
	self.rc.metaServer.setstsize(self.rc.serialize(path),self.rc.serialize((len(self.data[path])-1)*BLOCK_SIZE+len(self.data[path][-1])))
	self.rc.release()
	self.data.pop(path)

        return len(data)
			
		



class remoteClient:
	# class to take care of remote sessions 
	# also encoding decoding
    def __init__(self,lst):
	self.port_meta = lst[0]
	self.port_data = lst[1:]
	self.metaServer = ServerProxy('http://localhost:'+self.port_meta,allow_none=True)
	self.dataServer = []
	for port in self.port_data:
		self.dataServer.append(ServerProxy('http://localhost:'+port,allow_none=True))
	self.lock = 0
	self.n = len(lst)-1
	self.dsFlag = [False] * self.n

    def serialize(self,data):
	return pickle.dumps(Binary(data))

    def deserialize(self,data):
	return pickle.loads(data).data


# lock functions to serialize calls between sessions
    def acquire(self):
	while self.lock ==1:
		sleep(LOCK_WAIT_TIME)
	self.lock = 1

    def release(self):
		self.lock = 0
  
if __name__ == '__main__':
    if len(argv) < 4:
        print('usage: %s <mountpoint> <metaserver_port> <dataserver_port>*n' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(argv[1],remoteClient(argv[2:])), argv[1],foreground=True)


