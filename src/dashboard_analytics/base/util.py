from dotenv import load_dotenv
#from dashboard_analytics.logger.aix_db_logger import get_aix_ms_db_logger as db_logger
from dashboard_analytics.logger.logger import get_aix_ms_logger as common_logger
from dashboard_analytics.settings.constants import Constants
from dashboard_analytics.settings.env_vars import EnvironmentVars


def set_dev_environ_vars(env_path=None):
    if env_path is None:
        env_path = ".env.{env}".format(env="development")
    return load_dotenv(dotenv_path=env_path)


class UtilBase(object):
    def __init__(self, dev=False):
        self.dev = dev

        if dev:
            set_dev_environ_vars()
        self.constants = Constants()
        self.env_vars = EnvironmentVars()
        self.logger = common_logger(self.env_vars.APP_NAME)
        #self.db_logger = db_logger(f"{self.env_vars.APP_NAME}_db")
