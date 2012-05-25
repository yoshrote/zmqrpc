"""
server.py: Implements the ZMQRPCServer class. Inherit this class to 
transform your class into a multi-threaded 0MQ server.
"""

import sys, time
if sys.version < '2.6':
    sys.exit('ERROR: Sorry, python 2.6 is required for the way this module uses threading.')

import zmq, gevent, threading, os, sys, traceback

from errors    import BadConfig, NotExported, CannotExport
from serialize import get_serializer

FORBIDDEN_METHODS = [
    '__init__',
    'serve_forever', 
    'shutdown'
]

def export(method):
    """ Decorate instance methods with this decorator to export them
    as RPC methods. """
    if method.__name__ in FORBIDDEN_METHODS:
        raise CannotExport("'%s()' cannot be exported." % method)
    setattr(method, 'exported', True)
    return method


# ======================= Worker ======================== #

def worker_factory(multi_class, event_class):
    class MultiClass(multi_class):
        def __init__(self, name, serv):
            super(MultiClass, self).__init__()  
            self.name       = name
            self.serv       = serv
            self.worker_url = serv.url_worker_sock
            self.context    = serv.context
            self.serializer = get_serializer(serv.serializer)
            self.socket     = self.context.socket(zmq.REP)
            self.stop_flag  = event_class()
            self._debug     = self.serv._debug
        
            # Stats.
            self.op_counter   = {}
            self.last_success = {}
            self.last_fail    = {}

        def stop(self):
            """ Sets stop flag and closes 0mq socket. """
            print "closing worker socket"
            self.stop_flag.set()
            self.socket.close()

        def stopped(self):
            """ Checks stop flag. """
            return self.stop_flag.isSet()
    
        def status(self):
            return {
                'health': 'ok',
                'ops'   : self.op_counter,
                'last_success': self.last_success,
                'last_fail'   : self.last_fail
            }
    
        def inc_success(self, method):
            if not method in self.op_counter:
                self.op_counter[method] = {'success': 0, 'fail': 0}
            self.op_counter[method]['success'] += 1
    
        def inc_fail(self, method):
            if not method in self.op_counter:
                self.op_counter[method] = {'success': 0, 'fail': 0}
            self.op_counter[method]['fail'] += 1

        def _run(self):
            """ Run worker. """
            self.socket.connect(self.worker_url)
            serv_name = self.serv.__class__.__name__

            while not self.stopped():
                payload = None
                try:
                    payload = self.socket.recv()
                    if self._debug:
                        print "%s: received request" % self.name
                except zmq.ZMQError as e:
                    if self._debug:
                        print "%s: error encountered during recv: %s" % (self.name, str(e))
                    self.stop()
                    continue
                
                payload = self.serializer.unpack(payload)
                method  = str(payload['method'])
                args    = payload.get('args', [])
                kwargs  = self._str_keys(payload.get('kwargs', {}))
            
                try:
                    # Not defined method.
                    if not hasattr(self.serv, method):
                        raise NotExported("'%s()' is not exported." % method)
    
                    # Get method.
                    fn = getattr(self.serv, method)
                
                    # Check if it is exported.
                    if not hasattr(fn, 'exported') or not fn.exported:
                        raise NotExported("'%s()' is not exported." % method)
                
                    # Execute.
                    result = fn(*args, **kwargs)
                    payload = {
                        'success': True,
                        'result' : result,
                    }
                    payload = self.serializer.pack(payload)
                    self.socket.send(payload)
                    self.inc_success(method)
                    self.last_success['method'] = method
                    self.last_success['time'] = time.time()
                    if self._debug:
                        print "%s: send reply success" % self.name
                except Exception as e:
                    if self._debug:
                        print "%s: error occured while serving request: %s" % (self.name, str(e))
                    trace   = traceback.format_exc()
                    payload = {
                        'success': False,
                        'result' : None,
                        'exc': {
                            'trace': trace,
                            'msg'  : str(e),
                            'cls'  : e.__class__.__name__
                        }
                    }
                    payload = self.serializer.pack(payload)
                    self.socket.send(payload)
                    self.inc_fail(method)
                    self.last_fail['method'] = method
                    self.last_fail['time'] = time.time()
    
        def _str_keys(self, d):
            """ Convert dict keys to str. """
            kwargs = {}
            for k, v in d.iteritems():
                kwargs[str(k)] = v
            return kwargs
    
    return MultiClass


_ThreadWorker = worker_factory(threading.Thread, threading.Event)
class ThreadWorker(_ThreadWorker)
    def run(self):
        """ Run worker. """
        self._run()

GreenWorker = worker_factory(gevent.Greenlet, gevent.Event)

# ======================= ZMQRPCServer ======================== #


class ZMQRPCServer(object):
    """
    Inherit this class to transform your class into a multi-threaded 0MQ server.
    """
    def __init__(self, bind="tcp://127.0.0.1:5000", worker_bind="inproc://workers", worker_class=ThreadWorker, serializer="msgpack"):
        """ Init the RPC server. This method _must_ be called from
        the child class if it overrides the __init__ method. """
        if not bind:
            raise BadConfig("Bind address is not defined.")
        
        # Set to true to get verbose output for what's happening.
        self._debug = False

        self.serializer = serializer
        self.worker_class = worker_class
        self.url_worker_sock = worker_bind
        self.url_client_sock = bind
        
        self.context = zmq.Context(1)
        
        # Socket to talk to clients
        if self._debug:
            print "binding client socket at %s" % self.url_client_sock
        self.client_sock = self.context.socket(zmq.ROUTER)
        self.client_sock.bind(self.url_client_sock)
        
        # Socket to talk to workers
        if self._debug:
            print "binding workers socket at %s" % self.url_worker_sock
        self.worker_sock = self.context.socket(zmq.DEALER)
        self.worker_sock.bind(self.url_worker_sock) 

        self.workers = []

    def serve_forever(self, workers=2, worker_class=Worker):
        """ Start server and serve. """
        if self._debug:
            print "spawning %s workers" % str(workers)
        for i in range(workers):
            name   = "worker_%s" % str(i)
            worker = self.worker_class(name, self)
            self.workers.append(worker)
            worker.start()
            if self._debug:
                print "spawned %s" % name
        try:
            zmq.device(zmq.QUEUE, self.client_sock, self.worker_sock)
            if self._debug:
                print "this shouldn't be happening. shutting down."
            self.shutdown()
        except KeyboardInterrupt:
            if self._debug:
                print "keyboard interrupt"
            self.shutdown()
        
    def shutdown(self):
        """ Shut down the server and running threads. """
        if self._debug:
            print "closing dealer client socket"
        
        self.client_sock.close()
        
        if self._debug:
            print "closing dealer worker socket"
        
        self.worker_sock.close()
        
        if self._debug:
            print "terminating dealer 0mq context"
        
        self.context.term()
        
        if self._debug:
            print "waiting for workers to exit"
        
        for w in self.workers:
            w.join()
        print "bye"
        
    # Pre-defined methods.

    @export
    def __ping__(self):
        """ Check connectivity. """
        return "pong"
    
    @export
    def __serverstatus__(self):
        status = {}
        for t in self.workers:
            status[t.name] = t.status()
        return status
