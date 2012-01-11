import unittest

from client import ZMQRPC
from server import ZMQRPCServer, export
from errors import RemoteError, NotExported

class TestServerTests(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		cls.client = ZMQRPC("tcp://localhost:5000")

	def test_hello(self):
		self.assertEquals(self.client.hello(), "hello")
	
	def test_pow2(self):
		self.assertEquals(self.client.pow2(2), 4)
	
	def test_add(self):
		self.assertEquals(self.client.add(2,4), 6)
	
	def test_null(self):
		self.assertEquals(self.client.get_null(), None)
	
	def test_list_mult_two(self):
		self.assertListEqual(self.client.list_mult_two([1,2,3]), [2,4,6])
	
	def test_dict_mult_two(self):
		self.assertDictEqual(self.client.dict_mult_two({'a':1, 'b':2}), {'a': 2, 'b': 4})

	def test_ping(self):
		self.assertEquals(self.client.__ping__(), "pong")
	
	def test_not_exposed_method(self):
		self.assertRaises(NotExported, self.client.swim)
	
	def test_random_exception_in_server_method(self):
		self.assertRaises(RemoteError, self.client.random_exception)
	
	def test_many_ints(self):
		for i in range(10000):
			self.assertEquals(self.client.pow2(i), i**2)
	
	def test_many_lists(self):
		for i in range(10000):
			self.assertEquals(self.client.list_mult_two( [i,i+1,i+2]), [i*2, (i+1)*2, (i+2)*2] )

if __name__ == "__main__":
	unittest.main()
