import socket
import selectors
import json
import pickle
import xml.etree.ElementTree as XM
"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from typing import Any


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self._host = "localhost"
        self._port = 5000
        self.address = (self._host, self._port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sel = selectors.DefaultSelector()
        self.sock.connect(self.address)
        self.sel.register(self.sock, selectors.EVENT_READ, self.pull)
        self.topic = topic
        self._type = _type

        serialization_type = self.__class__.__name__
        msg_size = str(serialization_type)
        msg_json = json.dumps({"method": "ACK", "Serializer": msg_size}).encode('utf-8')
        self.sock.send(len(msg_json).to_bytes(2, "big") + msg_json)

        if self._type == MiddlewareType.CONSUMER:
            message = self.encode('SUBSCRIBE', self.topic, topic) 
            self.sock.send(len(message).to_bytes(2, "big") + message)


    def push(self, value):
        """Sends data to broker."""
        message = self.encode('PUBLISH', self.topic, value)
        self.sock.send(len(message).to_bytes(2, "big")   + message)  

    def pull(self) -> (str, Any):
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""
        header = self.sock.recv(2)
        if header:
            head = int.from_bytes(header, "big")
            data = self.sock.recv(head)
            if data:
                _, topic, msg = self.decode(data)
                return topic, msg

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        message = self.encode('LIST_TOPICS',self.topic,'')   
        self.sock.send(len(message).to_bytes(2, "big")  + message)

    def cancel(self):
        """Cancel subscription."""
        message = self.encode('UNSUBSCRIPTION',self.topic,self.topic) 
        self.sock.send(len(message).to_bytes(2, "big")    + message)


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    def __init__ (self,topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic,_type)

    def encode(self, method, topic, msg):
        message =   {'method':method,'topic':topic,'msg':msg}
        return (json.dumps(message)).encode('utf-8')

    def decode(self, data):
        data = data.decode('utf-8')
        data = json.loads(data)
        return data['method'], data['topic'], data['msg']

class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__ (self,topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic,_type)

    def encode(self, method, topic, msg):
        return ('<?xml version="1.0"?><data method="%(method)s" topic="%(topic)s"><msg>%(msg)s</msg></data>' % {'method':method,'topic':topic,'msg':msg}).encode('utf-8')
        
    def decode(self, data):
        data = data.decode('utf-8')
        data = XM.fromstring(data)
        data_temp = data.attrib
        return data_temp['method'], data_temp['topic'], data.find('msg').text


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__ (self,topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic,_type)

    def encode(self, method, topic, msg):
        return pickle.dumps({'method':method,'topic':topic,'msg':msg})

    def decode(self, data):
        msg = pickle.loads(data)
        return  msg["method"], msg["topic"], msg["msg"]