#Nakas Konstantinos-Panagiotis	cse32501@cs.uoi.gr	2501  
#Sunergaths: Katsantas Thwmas 

	
# Event-driven code that behaves as either a client or a server
# depending on the argument.  When acting as client, it connects 
# to a server and periodically sends an update message to it.  
# Each update is acked by the server.  When acting as server, it
# periodically accepts messages from connected clients.  Each
# message is followed by an acknowledgment.
#
# Tested with Python 2.7.8 and Twisted 14.0.2
#
import optparse

from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet import reactor
import time

Update_Peer = list()
transmission = 0
file_report = 0

def parse_args():
	usage = """usage: %prog [options] [client|server] [hostname]:port

	python peer.py server 127.0.0.1:port """

	parser = optparse.OptionParser(usage)

	_, args = parser.parse_args()

	peertype, addresses = args

	def parse_address(addr):
		if ',' in addr:
			addr1,addr2 = addr.split(',',1)
			host,port = addr1.split(':', 1)
			host2,port2 = addr2.split(':', 1)
			return host, int(port),host2,int(port2)
		else:
			host, port = addr.split(':', 1)
			return host, int(port)

		if not port.isdigit():
			parser.error('Ports must be integers.')

		

	return peertype, parse_address(addresses)


class Peer(Protocol):

	acks = 0
	sended_acks = 0
	connected = False
	update_counter = 0
	LC = list()
	messages = list()
	messages_acks = list()

	def __init__(self, factory, peer_type):
		self.pt = peer_type
		self.factory = factory

		if self.pt == 1:	
			self.update_counter = 2
		elif self.pt == 2:
			self.update_counter = 4
		for i in range(3):
			self.LC.append(0)
		for i in range(40):
			self.messages_acks.append(0)

	def connectionMade(self):
		global transmission, Update_Peer
		print "Connected from", self.transport
		Update_Peer.append(self)
		if len(Update_Peer) ==  2:
			print("First peer")
			transmission = transmission + 1
			reactor.callLater(3, self.sendUpdate, 0)
		try:
			self.transport.write('<connection up>')
			self.connected = True
		except Exception, e:
			print e.args[0]
		self.ts = time.time()

	def sendUpdate(self,update_counter):
		global transmission, Update_Peer
		update_counter = update_counter + 1
		for i in Update_Peer:
			print "Sending update"+ str(update_counter)
			try:
				self.LC[int(self.pt)] += 1
				i.transport.write('<update '+str(update_counter)+'> from Update_node:' +str(self.pt) +'with Update_timestamp:'+str(self.LC[int(self.pt)])+'$')
			except Exception, ex1:
				print "Exception trying to send: ", ex1.args[0]
		if self.connected == True and transmission<20:
			transmission = transmission + 1
			reactor.callLater(3, self.sendUpdate,update_counter)

	def sendAck(self,node,message_id):
		self.ts = time.time()
		try:
			self.sended_acks += 1
			print "sendAck " + str(self.sended_acks)
			self.LC[int(self.pt)] += 1
			node.transport.write('<Ack'+str(message_id)+'> from Ack_node:'+str(self.pt)+'with Ack_timestamp:'+str(self.LC[int(self.pt)])+'\n\n')
		except Exception, e:
			print e.args[0]


	def dataReceived(self, data):
		global Update_Peer,file_report
		if '<update' in data:
			self.LC[int(self.pt)] = max(int(data.split('Update_timestamp:')[1].split('$')[0]),self.LC[int(self.pt)]) + 1
			self.LC[int((data.split('Update_node:')[1]).split('with')[0])] = int(data.split('Update_timestamp:')[1].split('$')[0])
			flag = 0
			if len(self.messages) == 0:
				self.messages.append(data)
				flag = 1
			if (flag == 0):
				flag1 = 0
				for i in range(len(self.messages)):
					if int(self.messages[i].split('Update_timestamp:')[1].split('$')[0]) > int(data.split('Update_timestamp:')[1].split('$')[0]):
						self.messages.insert(i,data)
						flag1 = 1
						break
				if flag1 == 0:
					self.messages.append(data)
			for i in Update_Peer:
				self.sendAck(i,data.split('<update ')[1].split('>')[0])
		if '<Ack' in data:
			self.LC[int(self.pt)] = max(int(data.split('Ack_timestamp:')[1].split('\n')[0]),self.LC[int(self.pt)]) + 1
			self.LC[int((data.split('Ack_node:')[1]).split('with')[0])] = int(data.split('Ack_timestamp:')[1].split('\n')[0])

			self.messages_acks[int(data.split('<Ack')[1].split('>')[0])] = 1

			counter_pop = 0
			for i in range(len(self.messages)):
				if self.messages_acks[int(self.messages[i].split('<update ')[1].split('>')[0])] == 1:	
					file_report.write(self.messages[i] + '\n')
					counter_pop += 1					
				else:
					break
			for i in range(counter_pop):
				self.messages.pop()
			self.acks += 1	
	

	def connectionLost(self, reason):
		print "Disconnected"
		self.connected = False
		self.done()

	def done(self):
		self.factory.finished(self.acks)


class PeerFactory(ClientFactory):

	def __init__(self, peertype, fname):
		print '@__init__'
		self.pt = peertype
		self.acks = 0
		self.fname = fname
		self.records = []

	def finished(self, arg):
		self.acks = arg
		self.report()

	def report(self):
		print 'Received '+str(self.acks)+' acks'

	def clientConnectionFailed(self, connector, reason):
		print 'Failed to connect to:', connector.getDestination()
		self.finished(0)

	def clientConnectionLost(self, connector, reason):
		print 'Lost connection.  Reason:', reason

	def startFactory(self):
		print "@startFactory"
		if self.fname != '':
			self.fp = open(self.fname, 'w+')

	def stopFactory(self):
		print "@stopFactory"
		if self.fname != '':
			self.fp.close()

	def buildProtocol(self, addr):
		print "@buildProtocol"
		protocol = Peer(self, self.pt)
		return protocol


if __name__ == '__main__':
	peer_type, address = parse_args()

	file_report = open("delivered-message-"+str(peer_type)+".txt",'w')

	if peer_type == '0':
		factory = PeerFactory('0', 'log')
		reactor.listenTCP(2501, factory)
		print "Starting node 0 @" + '127.0.0.1' + " port " + str('2501')
	elif peer_type == '1':
		factory = PeerFactory('1', 'log')
		reactor.listenTCP(2502, factory)
		print "Starting node 1 @" + '127.0.0.1' + " port " + str('2502')

		factory = PeerFactory('1', '')
		host, port = address
		print "Connecting to host " + host + " port " + str(port)
		reactor.connectTCP(host, port, factory)
	else:
		factory = PeerFactory('2', 'log')
		reactor.listenTCP(2503, factory)
		print "Starting node 2 @" + '127.0.0.1' + " port " + str('2503')

		factory = PeerFactory('2', '')
		host, port, host2, port2 = address
		print "Connecting to host " + host + " port " + str(port)
		reactor.connectTCP(host, port, factory)

		factory = PeerFactory('2', '')
		
		print "Connecting to host " + host2 + " port " + str(port2)
		reactor.connectTCP(host2, port2, factory)

	reactor.run()
