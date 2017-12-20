
# Shrivinayak Bhat
# University of Florida


#This is the metaserver code for distributed remote file system.

# Usage: python metaserver.py 2222







import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import sleep

from SocketServer import ThreadingMixIn




class SimpleHT(ThreadingMixIn):
  def __init__(self):
    self.files = {}
    self.hash = {}

  def count(self):
    return len(self.files)

  def get(self, key):
    rv = {}
    key = key.data
    if key in self.files:
      rv = Binary(self.files[key])
    return rv

  def matchKey(self, key):
    key = self.deserialize(key)
    print 'matchKey -',key
    if key in self.files.keys():
	return self.serialize(1)
    else:
	return self.serialize(0) 

  def put(self, key, value):
    key = self.deserialize(key)
    value = self.deserialize(value)
    print('key:',key)
    print('value:',value)
    self.files[key] = value
    return True

  def serialize(self,data):
    return pickle.dumps(Binary(data))

  def deserialize(self,data):
    return pickle.loads(data).data


  def chmod(self,path,mode):
    path = self.deserialize(path)
    mode = self.deserialize(mode)
    self.files[path]['st_mode'] &= 0o770000
    self.files[path]['st_mode'] |= mode
    return self.serialize(0)


  def chown(self, path, uid, gid):
    path = self.deserialize(path)
    uid = self.deserialize(uid)
    gid = self.deserialize(gid)
    self.files[path]['st_uid'] = uid
    self.files[path]['st_gid'] = gid
    return self.serialize(0)


  def create(self, path, mode,time):
    path = self.deserialize(path)
    mode = self.deserialize(mode)
    time = self.deserialize(time)

    self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time, st_mtime=time,
                                st_atime=time,folder=False)
    self.files[self.parent(path)]['st_nlink'] +=1
    return self.serialize(0)

  def getattr(self, path):
    path = self.deserialize(path)
    return self.serialize(self.files[path])

  def getxattr(self, path, name):
    path = self.deserialize(path)
    name = self.deserialize(name)

    attrs = self.files[path].get('attrs', {})
    try:
      return self.serialize(attrs[name])
    except KeyError:
      return self.serialize('error')     # Should return ENOATTR


  def listxattr(self, path):
    path = self.deserialize(path)
    attrs = self.files[path].get('attrs', {})
    return self.serialize(attrs.keys())

  def mkdir(self, path, mode,time):

    path = self.deserialize(path)
    mode = self.deserialize(mode)
    time = self.deserialize(time)

    self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time, st_mtime=time,
                                st_atime=time,folder=True)
    self.files[self.parent(path)]['st_nlink'] +=1

    return self.serialize(0)



  def readdir(self, path, fh):
    path = self.deserialize(path)
    fh = self.deserialize(fh)
    print 'readdir called ----------------'
    print 'path: ',path
    print 'fh: ',fh
    #modified

    if path[-1]!='/':
            path+='/'
    #initializing the list that will be returned at the end of the function
    directories = ['.','..']

    print 'path',path
    #iterating through all the elements of self.files
    for p in self.files.keys():
      print p
      #if path matches the key ( eg: /dir1/dir2 in /dir1/dir2/file1.txt)
      if path==p[:len(path)] and path!=p and path[:-1]!=p:
      #if path in p and path!=p:
        temp = p[len(path):]
        if '/' in temp :
          temp = temp.split('/')[0]

          #if the directory not already part of the returning list.
          if temp not in directories:
            directories.append(temp)
        else:
          if temp not in directories:
            directories.append(temp)
    import pprint 
    pprint.pprint(self.files)
    return self.serialize(directories)




  def removexattr(self, path, name):
    path = self.deserialize(path)
    name = self.deserialize(name)

    attrs = self.files[path].get('attrs', {})

    try:
      del attrs[name]
    except KeyError:
      pass        # Should return ENOATTR
    return self.serialize(0)

  def rename(self, old, new):
    old = self.deserialize(old)
    new = self.deserialize(new)

    for e in self.files.keys():
      if old in e:
	if old == e[:len(old)]:
          self.files[e.replace(old,new)] = self.files.pop(e)
    return self.serialize(0)


  def rmdir(self, path):
    path = self.deserialize(path)
    self.files[self.parent(path)]['st_nlink'] -=1
    self.files.pop(path)
    return self.serialize(0)

  def setxattr(self, path, name, value):
    path = self.deserialize(path)
    name = self.deserialize(name)
    value = self.deserialize(value)

    attrs = self.files[path].setdefault('attrs', {})
    attrs[name] = value
    return self.serialize(0)

  def symlink(self, target, source):
    target = self.deserialize(target)
    source = self.deserialize(source)

    self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,st_size=len(source))
    return self.serialize(0)


  def truncate(self,path,length):
    path = self.deserialize(path)
    length = self.deserialize(length)
    self.files[path]['st_size'] = length
    return self.serialize(0) 

  def unlink(self, path):
    path = self.deserialize(path)
    self.files.pop(path)
    self.files[self.parent(path)]['st_nlink'] -=1
    return self.serialize(0)

  def utimens(self, path, times,now):
    path = self.deserialize(path)
    times = self.deserialize(times)
    now = self.deserialize(now)

    if times == 0:
	atime,mtime=times
    else:
	atime,mtime = now,now

    self.files[path]['st_atime'] = atime
    self.files[path]['st_mtime'] = mtime
    return self.serialize(0)

  def setstsize(self,path,value):
	path = self.deserialize(path)
	value = self.deserialize(value)
	self.files[path]['st_size'] = value
	return 1

  def parent(self, path):
    temp='/'.join(path.split('/')[:-1])
    if temp=='':
      return '/'
    else:
      return temp


  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.files = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.files, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.files
    return True

  def keyCount(self):
    print self.files
    return len(self.files)
  
  def shelve_recover(self,server_id,ports):

    keys = self.files.keys()
    if '/' in keys:
    	keys.remove('/')
    #remove folders
    for key in keys:
	if self.files[key]['folder']:
		keys.remove(key)
    return keys

  
  #functions related to hash
  def getHash(self,path):
	path = self.deserialize(path)
	if path in self.hash.keys():
		return self.serialize(self.hash[path])
	else:
		return self.serialize(False)

  def putHash(self,path,value):
	path = self.deserialize(path)
	value = self.deserialize(value)
	self.hash[path] = value
	return True

  def removeHash(self,path):
	path = self.deserialize(path)
	if path in self.hash.keys():
		self.hash.pop(path)
		return True
	else:
		return False
  def getHashKeys(self):
	return self.serialize(self.hash.keys())



def main():
  try:
	serve(int(sys.argv[1]))
  except:
	print "usage: python metaserver.py <port-no>"
# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port),allow_none=True)
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.matchKey)
  file_server.register_function(sht.put)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  file_server.register_function(sht.setstsize)
  file_server.register_function(sht.chmod)
  file_server.register_function(sht.chown)
  file_server.register_function(sht.utimens)
  file_server.register_function(sht.unlink)
  file_server.register_function(sht.truncate)
  file_server.register_function(sht.symlink)
  file_server.register_function(sht.setxattr)
  file_server.register_function(sht.rmdir)
  file_server.register_function(sht.rename)
  file_server.register_function(sht.removexattr)
  file_server.register_function(sht.readdir)
  file_server.register_function(sht.mkdir)
  file_server.register_function(sht.listxattr)
  file_server.register_function(sht.getattr)
  file_server.register_function(sht.getxattr)
  file_server.register_function(sht.keyCount)
  file_server.register_function(sht.create)
  file_server.register_function(sht.shelve_recover)
  file_server.register_function(sht.getHash)
  file_server.register_function(sht.putHash)
  file_server.register_function(sht.removeHash)
  file_server.register_function(sht.getHashKeys)

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
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234",allow_none=True))
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
