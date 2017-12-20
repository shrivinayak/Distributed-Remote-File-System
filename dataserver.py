# Shrivinayak Bhat
# University of Florida


# This is the dataserver code for distributed remote file system.

# Usage: python dataserver.py <data-server-no> <port1> <port2> ... <portn>
# Eg: python dataserver.py 0 3333 4444 5555 6666



import os
import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary, ServerProxy
from collections import defaultdict
import shelve
import hashlib
from random import randint

BLOCK_SIZE = 8
SERVER_ID = ''
RECOVER = False
PORTS = []
REPLICATION_FACTOR = 3
HASH = {}

# Presents a HT interface
class SimpleHT:
  def __init__(self):
    self.data = defaultdict(bytes)
    try:
    	s = shelve.open('data store '+SERVER_ID+'.db')
    	for key in s.keys():
		self.data[key] = s[key]
    except Exception as e:
	print e

  def count(self):
    return len(self.data)


# check if key is there
  def matchKey(self, pattern):

    # Default return value
    print "search key=",pattern
    keys = []

    for key in self.data.keys():
	if pattern in key:
		keys.append(key)
    print "returning",keys
    return Binary(str(keys))

# method to simulate read
  def read(self, path, size, offset):
    print self.data
    path=deserialize(path)
    size=deserialize(size)
    offset=deserialize(offset)
    #modified
    #picking up elements from the list which fall in the range (offset,offset+size)
    if path not in self.data.keys():
            return serialize('')

    if len(self.data[path]) == 0:
            return serialize('')
    temp = self.data[path][int(offset/BLOCK_SIZE)+1:int((offset+size)/BLOCK_SIZE)]

    #first and the last block of the (offset,offset+size) are added separately because they need further slicing 
    temp = list(self.data[path][int(offset/BLOCK_SIZE)][offset%BLOCK_SIZE:])+temp+self.data[path][:((offset+size)%BLOCK_SIZE)+1]
    return serialize(''.join(temp))

# method to read link from self.data
  def readlink(self, path):
    #modified
    path = deserialize(path)
    print self.data

    return serialize(self.data[path])

  def rename(self,old,new):
    global HASH
    print self.data
    old = deserialize(old)
    new = deserialize(new)
    global SERVER_ID
    s = shelve.open('data store '+SERVER_ID+'.db')

    for e in self.data.keys():
      if old in e:
	if old == e[:len(old)]:
          temp = self.data.pop(e)
	  s.pop(str(e))
	  newkey = e.replace(old,new)
          self.data[newkey] = temp
    	  s[str(newkey)] = temp
	  if e in HASH.keys():
	    HASH[e.replace(old,new)] = HASH.pop(e)
    s.close()
    return serialize(0)


  def symlink(self,target,source):

    target = deserialize(target)
    source = deserialize(source)
    print self.data

    self.data[target] = source
    shelve_update(str(target),str(source))


  def unlink(self, path):
    path = deserialize(path)

    if path in self.data.keys():
      del self.data[path]
      shelve_remove(path)

#xmlrpc for bulk receive 
  def putData(self,path,data):
    global RECOVER
    global HASH

    if path not in HASH.keys():
	HASH[path] = hash(path)

    while RECOVER:
        time.sleep(0.1)

    path = deserialize(path)
    data = deserialize(data)

    self.data[path]=data
    try:
    	shelve_update(str(path),data)
    except Exception as e:
	print 'shelve update error',e
    return serialize(True)

# xmlprc for bulk send
  def getData(self,path):
    global RECOVER
    while RECOVER:
        time.sleep(0.1)

    path = deserialize(path)
    if path not in self.data.keys():
	print 'key not found'
	return serialize(False)
    else:
	print 'getData called',path,self.data[path]
    	return serialize(self.data[path])

  def recoverCorrupt(self,path):
	path = deserialize(path)
	print 'block corruption: path:',path
	self.data.pop(path)
	recover_key(path)
	s = shelve.open('data store '+SERVER_ID+'.db')
	self.data[path] = s[path]
	s.close()
	print 'recovered ',path
	return True


  def ckeyRecover(self,path):
	print 'ckeyRecover'
	path = deserialize(path)
	path = str(path)
	data = distributedGet(path)
        print 'recovered data {',path,':',data,'}'
        data = distributedPut(str(path),data)
	self.data[path] = data
	return True

# xmlrpc to put a single block into the server
  def putBlock(self,path,blk,y):
	path = deserialize(path)
	blk = deserialize(blk)
	y = deserialize(y)
	
	self.data[path][y] = blk
	shelve_update(path,self.data[path])
		
# xmlrpc to receive a single block of data
  def getBlock(self,path,bno):
	path = str(deserialize(path))
	bno = int(deserialize(bno))
	return serialize(self.data[path][bno])

# to simulate corruption
  def corrupt(self,path):
	path = str(deserialize(path))
	if path not in self.data.keys():
		return False
	blk_no = randint(0,len(self.data[path])-1)

	self.data[path][blk_no] = self.data[path][blk_no][::-1]
	shelve_update(path,self.data[path])
	return 1+blk_no



#global functions

def serialize(data):
    return pickle.dumps(Binary(data))

def deserialize(data):
    return pickle.loads(data).data

# regerate original string from a 2d list of data blocks
def regenarate( dblocks,path):
	print dblocks
	global BLOCK_SIZE
	global HASH
        print 'entering regerate'
        metaServer = ServerProxy('http://localhost:'+str(2222),allow_none=True)
        length= deserialize(metaServer.getattr(serialize(path)))['st_size']

        block_count = length/BLOCK_SIZE
        if length % BLOCK_SIZE :
                block_count +=1

	if path not in HASH.keys():
		HASH[path] = hash(path)
		
        start = HASH[path]
        N = len(PORTS)
        data = []
        for i in range(1,int(block_count)+1):
                x = (start+i)%N
                temp = []
                for j in range(REPLICATION_FACTOR):

                        try:
				print "dblocks[x%N][0] = ",dblocks[x%N][0]
                                temp.append(dblocks[x%N][0])
                                dblocks[x%N] = dblocks[x%N][1:]
                                x+=1
                        except Exception as e:
                                ##print e
                                pass
                while 1:
                        if '' in temp:
                                temp.remove('')
                        else:
                                break
		if temp!=[]:
                	data.append(temp[0][:len(temp[0])-32])

	print 'recovered data ',data
        return data



    
def normalize(dblocks):
        largest = 0
        for line in dblocks:
                if len(line)>largest:
                        largest = len(line)
        for i in range(len(dblocks)):
                if len(dblocks[i])<largest:
                        dblocks[i]+=['']*(largest-len(dblocks[i]))



# method to recover data to shelve
def distributedGet(path):
	global PORTS
	global REPLICATION_FACTOR
	global SERVER_ID

	dataServer = []
	for port in PORTS:
		dataServer.append(ServerProxy('http://localhost:'+port,allow_none=True))

        dblocks = []
	flag = [False]*len(PORTS)
        for i in range(len(PORTS)):
		if i == int(SERVER_ID):
			dblocks.append([])
			continue
                try:
                        temp = deserialize(dataServer[i].getData(serialize(path)))

                        dblocks.append(temp)

                except Exception as e:
                        print('kerror',e)
                        dblocks.append([])
                        flag[i] = False


	
	normalize(dblocks)
        return regenarate(dblocks,path)


# method to send data 
def distributedPut(path,value):
	global HASH
	if path not in HASH.keys():
		HASH[path] = hash(path)
        x = HASH[path]
        N = len(PORTS) # No of data servers

        dblocks = []
        for i in range(N):
            dblocks.append([])

        dserver = x

	if not value:
		value = []	
        for blk in value:
                for j in range(REPLICATION_FACTOR):
                        dblocks[(dserver+j+1)%N].append(blk)
                dserver+=1

        dblocks[int(SERVER_ID)].append(hashlib.md5(''.join(dblocks[int(SERVER_ID)])).hexdigest())
	shelve_update(path,dblocks[int(SERVER_ID)])
	return dblocks[int(SERVER_ID)]
	




def recover_key(key):
	data = distributedGet(key)
	distributedPut(key,data)



#recover shelve db
def shelve_recover(metaServer):
	global SERVER_ID
	global RECOVER
	global PORTS

	RECOVER = True
	print '>checking for missing datastore.'
	keys = False
	keys = metaServer.shelve_recover(SERVER_ID,PORTS)
	
	print keys
	#step 1
	dataserver = []
	for port in PORTS:
		dataserver.append(ServerProxy('http://localhost:'+port,allow_none=True))
	
	try:
		s = shelve.open('data store '+SERVER_ID+'.db')
		lst = s.keys()
	except:
		os.remove('data store '+SERVER_ID+'.db')
		print('shelve db error/missing, full recovery triggered.')
		s = shelve.open('data store '+SERVER_ID+'.db')
		lst = s.keys()

	for key in keys:
		if key not in lst:
			recover_key(key)
		

	RECOVER = False
	print 'data recovered if any'

#remove element from shelve db
def shelve_remove(path):
	global SERVER_ID
        s = shelve.open('data store '+SERVER_ID+'.db')
        s.pop(str(path))
        s.close()

#update shelve db
def shelve_update(path,value):
	global SERVER_ID
	s = shelve.open('data store '+SERVER_ID+'.db')
	s[str(path)] = value
	s.close()
#take care of shelve, toplevel
def shelve_manager():
  try:
	metaServer =  ServerProxy('http://localhost:'+str(2222),allow_none=True)
  	keyCount = metaServer.keyCount()
  except Exception as e:
	print e
	exit('Error. metaserver not reachable.')

  print 'metaserver keyCount:',keyCount
  #opening shelve db
  
  shelve_recover(metaServer)


def main():
  global SERVER_ID
  global PORTS
  
  try:
  	SERVER_ID = sys.argv[1]
  except Exception as e:
	exit("invalid arguments:\n usage: python <filename> <data-server-no> <port1> <port2> ... <portn>")

  PORTS = sys.argv[2:]
  shelve_manager()
  serve(int(sys.argv[int(SERVER_ID)+2]))

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port),allow_none=True)
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.read)
  file_server.register_function(sht.readlink)
  file_server.register_function(sht.rename)
  file_server.register_function(sht.symlink)
  file_server.register_function(sht.unlink)
  file_server.register_function(sht.matchKey)
  file_server.register_function(sht.getData)
  file_server.register_function(sht.putData)
  file_server.register_function(sht.ckeyRecover)
  file_server.register_function(sht.getBlock)
  file_server.register_function(sht.putBlock)
  file_server.register_function(sht.corrupt)
  file_server.serve_forever()

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(51234, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
  main()
