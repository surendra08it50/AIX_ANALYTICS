import os

TO_BE_FILLED = "xxxx"


class EnvironmentVars(object):
    def __init__(self):
        # ENV CONFIGURATIONS
        self.APP_NAME = os.environ.get("APP_NAME", "aix")
        self.CLICKHOUSE_DB_NAME = os.environ.get("CLICKHOUSE_DB_NAME")
        self.CLICKHOUSE_DB_SCHEMA = os.environ.get("CLICKHOUSE_DB_SCHEMA")
        self.CLICKHOUSE_DB_USER = os.environ.get("CLICKHOUSE_DB_USER")
        self.CLICKHOUSE_DB_PASSWD = os.environ.get("CLICKHOUSE_DB_PASSWD")
        self.CLICKHOUSE_DB_URL = os.environ.get("CLICKHOUSE_DB_URL")
        self.DB_NAME = os.environ.get("DB_NAME")
        self.DB_SCHEMA = os.environ.get("DB_SCHEMA")
        self.DB_USER = os.environ.get("DB_USER")
        self.DB_PASSWD = os.environ.get("DB_PASSWD")
        self.DB_URL = os.environ.get("DB_URL")
        self.APP_KEY = os.environ.get("APP_KEY")

        # LOG LEVEL CONFIGS
        self.APP_LOG_LEVEL = os.environ.get("APP_LOG_LEVEL", "INFO")
        self.SQALCHEMY_LOG_LEVEL = os.environ.get("SQLALCHEMY_LOG_LEVEL", "INFO")
        self.LOG_FILE_PATH = os.environ.get("LOG_FILE_PATH", "../logs")

        # JWT
        self.JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY", TO_BE_FILLED)
        self.JWT_ENABLED = os.environ.get("JWT_ENABLED")
        self.JWT_EXPIRATION_TIME = os.environ.get("JWT_EXPIRATION_TIME")

        # Python environment name
        self.PYTHON_ENV_NAME = os.environ.get("PYTHON_ENV_NAME")

        # DEBUG
        self.dev = os.environ.get("DEV", False)
