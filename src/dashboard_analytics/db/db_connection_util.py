import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dashboard_analytics.settings.constants import Constants
from dashboard_analytics.settings.env_vars import EnvironmentVars
from dashboard_analytics.logger.logger import get_log_handler

engine = None

class DbConnectionUtil(object):
    def __init__(self):
        self.settings = EnvironmentVars()

    def get_database_engine(self):
        try:
            global engine
            if engine is None:
                url = "postgresql+psycopg2://{user}:{passwd}{url}".format(
                    user=self.settings.DB_USER,
                    passwd=self.settings.DB_PASSWD,
                    url=self.settings.DB_URL,
                )
                engine = create_engine(url, pool_size=50, echo=False)
                logger = logging.getLogger("sqlalchemy.engine")
                logger.setLevel(level=self.settings.SQALCHEMY_LOG_LEVEL)
                logger.addHandler(get_log_handler(self.settings.APP_NAME))
        except IOError as ex:
            raise ex
        return engine

    @staticmethod
    def get_db_session_scope():
        engine = DbConnectionUtil().get_database_engine()
        Session = sessionmaker(bind=engine, autocommit=False)

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
