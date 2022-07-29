"""Message Broker"""
from asyncio import events
import enum
from typing import Dict, List, Any, Tuple
import socket
import selectors
import json
import pickle
import xml
import xml.etree.ElementTree as XM

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self.address = (self._host, self._port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(self.address)
        self.sock.listen()
        print("Broker has been initaizlized and is now listening on port ", self._port)
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept)
        self.topics_msg = {}         
        self.users = {}             
        self.topics_users = {}       
        self.topics_producer = []     
    
    def parser(self, message, conn):
        if json.loads(message)["Serializer"] == 'JSONQueue':
            self.users[conn] = Serializer.JSON
        elif json.loads(message)["Serializer"] == 'PickleQueue':
            self.users[conn] = Serializer.PICKLE
        elif json.loads(message)["Serializer"] == 'XMLQueue':
            self.users[conn] = Serializer.XML
    

    def accept(self, sock):
        conn, addr = sock.accept() 
        print(addr, " Has connected to the broker")
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)
        header = conn.recv(2)   # checking if there is info in the message and defining a buffer
        if header:
            head = int.from_bytes(header, 'big')
            message = conn.recv(head).decode("UTF-8")
            if message:
                self.parser(message, conn)

        else: 
            print(conn ,' closed')                          
            self.sel.unregister(conn)                       
            conn.close()  
    
    def decodeMessages(self, data, conn):
        if self.users[conn] == Serializer.JSON:
            data = data.decode('utf-8')
            msg = json.loads(data)
            return msg['method'], msg['topic'], msg['msg']
        elif self.users[conn] == Serializer.XML:
            data = data.decode('utf-8')
            data = XM.fromstring(data)
            data_temp = data.attrib
            return data_temp['method'], data_temp['topic'], data.find('msg').text
        elif self.users[conn] == Serializer.PICKLE:
            msg = pickle.loads(data)
            return msg['method'], msg['topic'], msg['msg']

    def findMethod(self, method, topic, msg, conn):
        if method == 'PUBLISH':
            self.put_topic(topic, msg)
        elif method == 'SUBSCRIBE':
            self.subscribe(topic,conn, self.users[conn])
        elif method == 'UNSUBSCRIPTION':
            self.unsubscribe(topic,conn)
        elif method == 'LIST_TOPICS':
            self.send(conn,'LIST_TOPICS_ANSWER',topic, self.list_topics())

    def read(self, conn):
        try:
            header = conn.recv(2)
            if not header:
                raise ConnectionError()
            else:
                data = conn.recv(int.from_bytes(header, "big"))
                if data:
                    if conn in self.users:
                        method, topic, msg = self.decodeMessages(data, conn)
                        self.findMethod(method, topic, msg, conn)
        except ConnectionError:
            for i in self.topics_users:
                list_users = self.topics_users[i]
                j = 0 # Control Variable
                while j < len(list_users):
                    if list_users[j][0] == conn:
                        self.topics_users[i].remove(list_users[j])
                        break
                    j += 1
            self.sel.unregister(conn)
            conn.close()
            
    def encodeMessages(self, conn, method, topic, msg):
        if self.users[conn] == Serializer.JSON:
            return json.dumps({'method':method,'topic':topic,'msg':msg}).encode('utf-8')
        elif self.users[conn] == Serializer.PICKLE:
            return pickle.dumps({'method':method,'topic':topic,'msg':msg})
        elif self.users[conn] == Serializer.XML:
            return ('<?xml version="1.0"?><data method="%(method)s" topic="%(topic)s"><msg>%(msg)s</msg></data>' % {'method':method,'topic':topic,'msg':msg}).encode('utf-8')

    def send(self, conn, method, topic, msg):
        message = self.encodeMessages(conn, method, topic, msg)
            
        header = len(message).to_bytes(2, "big")
        package = header + message   
        conn.send(package)       
        print("Message sent to ", conn)


    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        return self.topics_producer

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic in self.topics_msg:
            return self.topics_msg[topic]
        return None

    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.topics_msg[topic] = value

        #verify if this topic is a subtopic of an existing topic 
        if topic not in self.topics_producer:
            self.topics_producer.append(topic)

        if topic not in self.topics_users:
            self.topics_users[topic] = []

            for topico in self.topics_users:
                print(topico)
                if topic.startswith(topico):
                    j = 0
                    while j < len(self.list_subscriptions(topico)):
                        i = self.list_subscriptions(topico)[j]
                        if i not in self.list_subscriptions(topic):
                            self.topics_users[topic].append(i)
                        j+=1

        if topic in self.topics_users:
            # send to all topics and "super-topics"
            for i in self.list_subscriptions(topic):
                self.send(i[0], 'MESSAGE', topic, value)

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        if topic in self.topics_users:
            return self.topics_users[topic]
        return []
        
    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""

        if topic not in self.topics_users:
            self.topics_users[topic] = []
            for topico in self.topics_users:
                print(topico)
                if topic.startswith(topico):
                    j = 0
                    while j < len(self.list_subscriptions(topico)):
                        i = self.list_subscriptions(topico)[j]
                        if i not in self.list_subscriptions(topic):
                            self.topics_users[topic].append(i)
                        j+=1
                            
        self.topics_users[topic].append((address, _format))
        
        if topic in self.topics_msg:
            self.send(address, 'LAST_MESSAGE', topic, self.get_topic(topic))        

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        if topic in self.topics_users:
            list_users = self.topics_users[topic]
            c=0
            while c < len(list_users):
                if list_users[c][0] == address:
                    self.topics_users[topic].remove(list_users[c])
                    break
                c+=1

    def run(self):
        """Run until canceled."""
        while not self.canceled:
            events = self.sel.select()
            for key, _ in events:
                callback = key.data
                callback(key.fileobj)