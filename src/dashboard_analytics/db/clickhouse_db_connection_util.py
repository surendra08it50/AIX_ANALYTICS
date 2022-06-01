import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dashboard_analytics.settings.constants import Constants
from dashboard_analytics.settings.env_vars import EnvironmentVars
from dashboard_analytics.logger.logger import get_log_handler

clk_house_engine = None

class DbConnectionUtil(object):
    def __init__(self):
        self.settings = EnvironmentVars()

    def get_database_engine(self):
        try:
            global clk_house_engine
            if clk_house_engine is None:
                url = "clickhouse://{user}:{passwd}{url}".format(
                    user=self.settings.CLICKHOUSE_DB_USER,
                    passwd=self.settings.CLICKHOUSE_DB_PASSWD,
                    url=self.settings.CLICKHOUSE_DB_URL,
                )
                clk_house_engine = create_engine(url, pool_size=50, echo=False)
                #logger = logging.getLogger("sqlalchemy.engine")
                #logger.setLevel(level=self.settings.SQALCHEMY_LOG_LEVEL)
                #logger.addHandler(get_log_handler(self.settings.APP_NAME))
        except IOError as ex:
            raise ex
        return clk_house_engine

    @staticmethod
    def get_db_session_scope():
        clk_house_engine = DbConnectionUtil().get_database_engine()
        Session = sessionmaker(bind=clk_house_engine, autocommit=False)

        session = Session()
        return session
        # try:
        #     yield session
        #     session.commit()
        # except:
        #     session.rollback()
        #     raise
        # finally:
        #     session.close
