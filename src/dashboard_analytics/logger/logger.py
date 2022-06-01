import logging
import logging.handlers as handlers
import os
import sys
from dashboard_analytics.settings.env_vars import EnvironmentVars

env_vars = EnvironmentVars()

# if log dir doesn't exists, then create one.
if not os.path.exists(env_vars.LOG_FILE_PATH):
    os.makedirs(env_vars.LOG_FILE_PATH)


def get_aix_ms_logger(svc, stdout=False):
    """
    function to return logger object.
    """
    logger = logging.getLogger(svc)

    # log to File.
    if not logger.hasHandlers():
        # Create the file if it does not exist
        if not os.path.exists(f"{env_vars.LOG_FILE_PATH}/{svc}.log"):
            open(f"{env_vars.LOG_FILE_PATH}/{svc}.log", "w").close()
            os.chmod(f"{env_vars.LOG_FILE_PATH}/{svc}.log", 0o777)

        logger.setLevel(level=env_vars.APP_LOG_LEVEL)
        logHandler = handlers.TimedRotatingFileHandler(
            f"{env_vars.LOG_FILE_PATH}/{svc}.log", when="D", interval=1, backupCount=2
        )
        logHandler.suffix = "%Y-%m-%d_%H_%M_%S.log"
        logHandler.setLevel(level=env_vars.APP_LOG_LEVEL)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)

        if stdout == True:
            stdoutHandler = logging.StreamHandler(sys.stdout)
            stdoutHandler.setLevel(level=env_vars.APP_LOG_LEVEL)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            stdoutHandler.setFormatter(formatter)
            logger.addHandler(stdoutHandler)
    return logger


def get_log_handler(svc):
    """
    file-rotating log handler.
    """
    # Create the file if it does not exist
    if not os.path.exists(f"{env_vars.LOG_FILE_PATH}/{svc}.log"):
        open(f"{env_vars.LOG_FILE_PATH}/{svc}.log", "w").close()
        os.chmod(f"{env_vars.LOG_FILE_PATH}/{svc}.log", 0o777)

    logHandler = handlers.TimedRotatingFileHandler(
        f"{env_vars.LOG_FILE_PATH}/{svc}.log", when="D", interval=1, backupCount=2
    )
    logHandler.suffix = "%Y-%m-%d_%H_%M_%S-pv.log"
    logHandler.setLevel(level=env_vars.SQALCHEMY_LOG_LEVEL)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logHandler.setFormatter(formatter)
    return logHandler
