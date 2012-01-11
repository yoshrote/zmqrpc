import unittest

from client import ZMQRPC
from server import ZMQRPCServer, export

class TestServer(ZMQRPCServer):

    @export
    def hello(self):
        return "hello"
    
    @export
    def add(self, x, y):
        return x + y
    
    @export
    def pow2(self, x):
        return self.helper(x)

    @export
    def list_mult_two(self, items):
        return map(lambda x: x*2, items)

    @export
    def dict_mult_two(self, items):
        for k, v in items.items():
            items[k] = v*2
        return items

    @export
    def get_null(self):
        return None
    
    @export
    def random_exception(self):
        class RandomException(Exception):
            pass
        raise RandomException("This is a random exception.")

    def helper(self, x):
        return x**2
    

if __name__ == "__main__":
    server = TestServer()
    server._debug = True
    server.serve_forever()
