class AixException(Exception):
    def __init__(self, message: str):
        self.type = self.__class__.__name__
        self.message = message

    def dict(self):
        return {
            "type": self.type,
            "message": self.message,
        }
