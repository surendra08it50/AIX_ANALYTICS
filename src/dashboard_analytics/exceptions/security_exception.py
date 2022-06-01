from dashboard_analytics.exceptions.base import AixException


class SecurityException(AixException):
    def __init__(self, message: str):
        super().__init__(message)

    def dic(self):
        return super().dict()


class UnauthorizedException(SecurityException):
    def __init__(self):
        msg = "Unauthorized"
        super().__init__(msg)


class ForbiddenException(SecurityException):
    def __init__(self):
        msg = "Forbidden"
        super().__init__(msg)
