import uuid
from datetime import datetime

from sqlalchemy import (
    ARRAY,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Text,
    JSON,
    CheckConstraint,
    Float,
    ForeignKeyConstraint
)

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from dashboard_analytics.db.db_connection_util import DbConnectionUtil
from dashboard_analytics.settings.constants import Constants
from dashboard_analytics.settings.env_vars import EnvironmentVars

constant = Constants()
env_vars = EnvironmentVars()
metadata_obj = MetaData(schema=env_vars.DB_SCHEMA)
Base = declarative_base(metadata=metadata_obj)


class AuditStamp:
    createdDate = Column("created_date", DateTime, default=datetime.now())
    updatedDate = Column(
        "updated_date",
        DateTime,
        default=datetime.now(),
        onupdate=datetime.now(),
    )
    createdBy = Column("created_by", String, default="admin")
    updatedBy = Column("updated_by", String, default="admin")


class User(Base, AuditStamp):
    __tablename__ = constant.USER_TABLE_NM
    # __table_args__ = {'schema': DB_SCHEMA}

    userId = Column(
        "user_id", String, primary_key=True, nullable=False, default=uuid.uuid4
    )
    userName = Column("user_name", String, unique=True, nullable=False)
    preferredName = Column("preferred_name", String, nullable=True)
    firstName = Column("first_name", String, nullable=False)
    lastName = Column("last_name", String, nullable=False)
    email = Column("email", String, nullable=False, unique=True)
    assignedRole = Column("assigned_role", String, nullable=False)
    appAllowedAccess = Column("app_allowed_access", ARRAY(String), nullable=False)
    password = Column("password", String, nullable=False)


class UserPreference(Base, AuditStamp):
    __tablename__ = constant.USER_PREFERENCES_TABLE_NM
    # __table_args__ = {'schema': DB_SCHEMA}
    user_pref_id = Column(
        "user_role_id", String, primary_key=True, nullable=False, default=uuid.uuid4
    )
    userName = Column("user_name", String,
                      ForeignKey('{user}.user_name'.format(user=constant.USER_TABLE_NM), ondelete="cascade"), unique=True,
                      nullable=False)
    settings = Column(
        "settings", JSON, nullable=True
    )


class AppListMaster(Base, AuditStamp):
    __tablename__ = constant.APP_LIST_MASTER_TABLE_NM
    # __table_args__ = {'schema': DB_SCHEMA}
    app_id = Column(
        "app_id", String, primary_key=True, nullable=False, default=uuid.uuid4
    )
    app_name = Column("app_name", String, unique=True, nullable=False)
    app_desc = Column("app_desc", String, nullable=True)
    active = Column("active", Integer, nullable=False, default=1)


class UserRole(Base, AuditStamp):
    __tablename__ = constant.USER_ROLE_MASTER_TABLE_NM
    # __table_args__ = {'schema': DB_SCHEMA}
    user_role_id = Column(
        "user_role_id", String, primary_key=True, nullable=False, default=uuid.uuid4
    )
    user_role_name = Column("user_role_name", String, unique=True, nullable=False)
    user_role_desc = Column("user_role_desc", String, nullable=True)
    active = Column("active", Integer, nullable=False, default=1)

class Log(Base):
    __tablename__ = "log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    endpoint = Column(String(200), nullable=True)
    datetime = Column(DateTime(timezone=False), nullable=False, default=datetime.now())
    status = Column(String(200), nullable=True)


Base.metadata.create_all(DbConnectionUtil().get_database_engine(), checkfirst=True)
