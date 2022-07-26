""" exceptions specific to yotpo-client"""

class YotpoError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class YotpoBadRequestError(YotpoError):
    pass

class YotpoServer5xxError(YotpoError):
    pass

class YotpoRateLimitError(YotpoError):
    pass


class YotpoUnauthorizedError(YotpoError):
    pass


class YotpoForbiddenError(YotpoError):
    pass


class YotpoBadGateway(YotpoServer5xxError):
    pass


class YotpoNotFoundError(YotpoError):
    pass


class YotpoTooManyError(YotpoRateLimitError):
    pass


class YotpoNotAvailableError(YotpoServer5xxError):
    pass


class YotpoGatewayTimeout(YotpoServer5xxError):
    pass

class YotpoInternalServerError(YotpoServer5xxError):
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": YotpoBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": YotpoUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": YotpoForbiddenError,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": YotpoNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    429: {
        "raise_exception": YotpoTooManyError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": YotpoInternalServerError,
        "message": "An error has occurred at Yotpo's end."
    },
    502: {
        "raise_exception": YotpoBadGateway,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": YotpoNotAvailableError,
        "message": "API service is currently unavailable."
    },
    504: {
        "raise_exception": YotpoGatewayTimeout,
        "message": "API service time out, please check Yotpo server."
    }
}