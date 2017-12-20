# Shrivinayak Bhat
# University of Florida


#This is a test bench to verify error correction feature






from xmlrpclib import ServerProxy, Binary
import hashlib
import sys
import pickle

def serialize(data):
	return pickle.dumps(Binary(data))

def deserialize(data):
        return pickle.loads(data).data



if __name__ == '__main__':
	if len(sys.argv)<3:
		print 'usage: python corruptblk.py <path> <port>'
		exit(1)


	path = sys.argv[1]
	port = sys.argv[2]

	dataServer = ServerProxy('http://localhost:'+port,allow_none=True)
	status = dataServer.corrupt(serialize(path))
	if status:
		print 'path found,data block',status-1,' corrupted'
	else:
		print 'path not found'
