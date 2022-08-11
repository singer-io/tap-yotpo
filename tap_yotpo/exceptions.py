""" exceptions specific to yotpo-client"""
TypeError
class ClientError(Exception):
    message = None
    def __init__(self, message=None, response=None):
        super().__init__(message or self.message)
        self.response = response


class Http400RequestError(ClientError):
    message = "Unable to process request"

class Http401RequestError(ClientError):
    message = "Invalid authorization credentials."

class Http403RequestError(ClientError):
    message = "User doesn't have permission to access the resource."

class Http404RequestError(ClientError):
    pass

class Http429RequestError(ClientError):
    message = "The API rate limit for your organisation/application pairing has been exceeded."

class Http500RequestError(ClientError):
    message = "An error has occurred at Yotpo's end."

class Http502RequestError(ClientError):
    message =  "Server received an invalid response."


class Http503RequestError(ClientError):
    message= "API service is currently unavailable"


class Http504RequestError(ClientError):
    message = "API service time out, please check Yotpo server."


# ERROR_CODE_EXCEPTION_MAPPING = {
#     400: {
#         "raise_exception": YotpoBadRequestError,
#         "message": "A validation exception has occurred."
#     },
#     401: {
#         "raise_exception": YotpoUnauthorizedError,
#         "message": "Invalid authorization credentials."
#     },
#     403: {
#         "raise_exception": YotpoForbiddenError,
#         "message": "User doesn't have permission to access the resource."
#     },
#     404: {
#         "raise_exception": YotpoNotFoundError,
#         "message": "The resource you have specified cannot be found."
#     },
#     429: {
#         "raise_exception": YotpoTooManyError,
#         "message": "The API rate limit for your organisation/application pairing has been exceeded."
#     },
#     500: {
#         "raise_exception": YotpoInternalServerError,
#         "message": "An error has occurred at Yotpo's end."
#     },
#     502: {
#         "raise_exception": YotpoBadGateway,
#         "message": "Server received an invalid response."
#     },
#     503: {
#         "raise_exception": YotpoNotAvailableError,
#         "message": "API service is currently unavailable."
#     },
#     504: {
#         "raise_exception": YotpoGatewayTimeout,
#         "message": "API service time out, please check Yotpo server."
#     }
# }
