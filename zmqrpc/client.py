"""
client.py: Client class to export a class to an zmqrpc queue or client.
"""

import zmq, time
import os, sys, traceback

from errors     import *
from serialize  import get_serializer    


# ====================== ZMQRPC ======================= #


class ZMQRPC(object):
    """
    ZMQRPC: Client class to export a class to a zmqrpc queue or client.
    """
    def __init__(self, server_url, timeout=10, serializer='msgpack'):
        """ Instantiate this class with a zmq url (eg 'tcp://127.0.0.1:5000') 
        and a timeout (in seconds) for method calls. Then call zmqrpc server 
        exported methods from the class. """
        if not server_url:
            raise BadConfig("Server url is not defined.")
        if timeout < 0:
            raise BadConfig("Illegal timeout value.")
        self.context    = zmq.Context()
        self.zmqsocket  = self.context.socket(zmq.REQ)
        self.server_url = server_url

        self.zmqsocket.connect(self.server_url)
           
        self.serializer = get_serializer(serializer)
        
        self.pollin = zmq.Poller()
        self.pollin.register(self.zmqsocket, zmq.POLLIN)
        
        self.pollout = zmq.Poller()
        self.pollout.register(self.zmqsocket, zmq.POLLOUT)
        
        # In millis.
        self.timeout = timeout * 1000

    def _request(self, payload):
        """ Send a zmq request to the server url. """
        payload = self.serializer.pack(payload)
        
        try:
            self.pollout.poll(timeout=self.timeout) 
            self.zmqsocket.send(payload, flags=zmq.NOBLOCK)
        except Exception as e:
            raise RequestFailure('Cannot send request. Error was: ' + str(e))

        msg_in = None
        try:
            socks = dict(self.pollin.poll(timeout=self.timeout))
            if socks and socks.get(self.zmqsocket) == zmq.POLLIN:
                msg_in = self.zmqsocket.recv(flags=zmq.NOBLOCK)
        except Exception as e:
            raise RequestFailure('Response timeout. Error was: ' + str(e))
        
        if msg_in == None:
            raise RequestFailure('No response.')
        
        return self.serializer.unpack(msg_in)
                    
    def __getattr__(self, name):
        """ Return a wrapper function, encapsulating the rpc call. """
        def rpc_wrapper(*args, **kwargs):
            payload = {
                'method': name,
                'args'  : args,
                'kwargs': kwargs
            }
            result = self._request(payload)
            if 'success' not in result:
                raise InvalidResponse("Cannot find 'success' attr.") 
            if not result['success']:
                exc_class  = result['exc']['cls']
                exc_msg    = result['exc']['msg']
                errors_mod = __import__('errors')
                if hasattr(errors_mod, exc_class):
                    raise getattr(errors_mod, exc_class)(exc_msg)
                else:
                    raise RemoteError(exc_msg)
            else:
                return result['result']
        return rpc_wrapper
 
