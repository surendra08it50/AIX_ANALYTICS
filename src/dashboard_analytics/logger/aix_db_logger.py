import logging
from logging import Handler, getLogger

from dashboard_analytics.base.db_service import ServiceBase
from dashboard_analytics.base.models import Log
from dashboard_analytics.settings.env_vars import EnvironmentVars

env_vars = EnvironmentVars()


class LogDbHandler(Handler):
    """
    for each log call, DB connection session will open , insert record.
    and then close the session.
    """

    def __init__(self, level=0):
        super().__init__(level)
        self.service_base = ServiceBase(Log)

    def emit(self, record):
        message = self.format(record)

        try:
            new_log = Log(
                endpoint=record.endpoint,
                datetime=record.datetime,
                status=record.status,
            )

            self.service_base.create_one(new_log)
        except Exception as err:
            print(err)
            raise err


def get_aix_ms_db_logger_handler():
    """
    how to use:
    use handler in logging
    """
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    db_handler = LogDbHandler()
    db_handler.setLevel(env_vars.APP_LOG_LEVEL)
    db_handler.setFormatter(formatter)

    return db_handler


def get_aix_ms_db_logger(svc, stdout=False):
    """
    function to return logger object.
    """
    logger = logging.getLogger(svc)
    logger.setLevel(level=env_vars.APP_LOG_LEVEL)
    if not logger.hasHandlers():
        logger.addHandler(get_aix_ms_db_logger_handler())

    return logger
