from dashboard_analytics.exceptions.base import AixException


class DbException(AixException):
    def __init__(self, message: str):
        super().__init__(message)

    def dic(self):
        return super().dict()
