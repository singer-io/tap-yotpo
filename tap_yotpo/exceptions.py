""" exceptions specific to yotpo-client"""

class ClientError(Exception):
    message = None
    def __init__(self, message=None, response=None):
        super().__init__(message or self.message)
        self.response = response


class Http400RequestError(ClientError):
    message = "Unable to process request"

class Http401RequestError(ClientError):
    message = "Invalid credentials provided"

class Http403RequestError(ClientError):
    message = "Insufficient permission to access resource"

class Http404RequestError(ClientError):
    message = "Resource not found"

class Http429RequestError(ClientError):
    message = "The API limit exceeded."

class Http500RequestError(ClientError):
    message = "Server Fault, Unable to process request"

class Http502RequestError(ClientError):
    message =  "Bad Gateway."


class Http503RequestError(ClientError):
    message= "Service is currently unavailable"


class Http504RequestError(ClientError):
    message = "API service time out"
