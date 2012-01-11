"""
ZMQRPC exceptions.
"""

class BaseError(Exception):

    def __init__(self, value):
     self.value = value
    
    def __str__(self):
     return str(self.value)
    
    def __unicode__(self):
     return unicode(self.value)

class CannotFindSerializer(BaseError):
    """ Raised when the requested paylaod serializer
    cannot be found. """

    def __init__(self):
        self.value = "Cannot find serializer." 
        BaseError.__init__(self, self.value)

class BadConfig(BaseError):
    """ Raised when some parameter was wrongly configured. """
    pass

class NotExported(BaseError):
    """ Raised when a called method is not exported. """
    pass

class CannotExport(BaseError):
    """ Raised when trying to export a method which is not allowed. """
    pass

class InvalidResponse(BaseError):
    """ Raised when the response received is not as expected. """
    pass

class RequestFailure(BaseError):
    """ Raised when the request to the server failed. """
    pass

class RemoteError(BaseError):
    """ Raised when the request to the server resulted in a server error. """
    pass



