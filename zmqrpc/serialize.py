"""
Support for multiple serialization methods. 
"""

import msgpack, simplejson, pickle
from errors import CannotFindSerializer

def get_serializer(type="msgpack"):
    type = type.capitalize()
    cls  = "%sSerializer" % type
    gs   = globals()
    if cls in gs:
        return gs[cls]()
    else:
        raise CannotFindSerializer()

class Serializer(object):
    # TODO
    def pack(self, payload):
        pass
    
    def unpack(self, payload):
        pass

class PickleSerializer(Serializer):

    def pack(self, payload):
        pickle.dumps(payload)
    
    def unpack(self, payload):
        pickle.loads(payload)

class JsonSerializer(Serializer):

    def pack(self, payload):
        simplejson.dumps(payload)
    
    def unpack(self, payload):
        simplejson.loads(payload)

class MsgpackSerializer(Serializer):

    def pack(self, payload):
        return msgpack.packs(payload)
    
    def unpack(self, payload):
        return msgpack.unpacks(payload, use_list=True)
