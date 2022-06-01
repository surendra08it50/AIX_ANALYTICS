from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, \
     ForeignKey, Boolean, event
from sqlalchemy.orm import scoped_session, sessionmaker, backref, relation
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, MetaData, literal
from clickhouse_sqlalchemy import Table, make_session, get_declarative_base, types, engines

from dashboard_analytics.db.clickhouse_db_connection_util import DbConnectionUtil
from dashboard_analytics.settings.constants import Constants
from dashboard_analytics.settings.env_vars import EnvironmentVars

constant = Constants()
env_vars = EnvironmentVars()
metadata = MetaData(bind=DbConnectionUtil().get_database_engine())
Base = get_declarative_base(metadata=metadata)


class ToolMetadata(Base):
    __tablename__ = 'tool_metadata'
    __table_args__ = (
        engines.MergeTree(order_by=['tool_name']),
        {'schema': 'aix_db'},
    )
    tool_name = Column(types.LowCardinality(String), primary_key=True)
    alias = Column(types.LowCardinality(String))
    tool_type = Column(types.LowCardinality(String))
    software_version = Column(types.LowCardinality(String))
    timezone_iana = Column(types.LowCardinality(String))
    timezone_microsoft = Column(types.LowCardinality(String))
    insertion_datetime = Column(types.DateTime64, nullable=False, default=str(datetime.now()))

    def getvals(self):
        temp_dict = {}
        temp_dict["tool_name"] = self.tool_name
        temp_dict["alias"] = self.alias
        temp_dict["tool_type"] = self.tool_type
        temp_dict["software_version"] = self.software_version
        temp_dict["timezone_iana"] = self.timezone_iana
        temp_dict["timezone_microsoft"] = self.timezone_microsoft
        temp_dict["insertion_datetime"] = self.insertion_datetime

        return temp_dict


class ChamberMetadata(Base):
    __tablename__ = 'chamber_metadata'
    __table_args__ = (
        engines.MergeTree(order_by=['wafer_loc_id']),
        {'schema': 'aix_db'},
    )
    wafer_loc_id = Column(types.LowCardinality(String), primary_key=True)
    tool_name = Column(types.LowCardinality(String))
    chamber_name = Column(types.LowCardinality(String))
    slot = Column(types.Int32)
    side = Column(types.Int32)
    chamber_type = Column(types.LowCardinality(String))
    alias = Column(types.LowCardinality(String))
    path = Column(types.LowCardinality(String))
    hasuserecipies = Column(types.LowCardinality(String))
    insertion_datetime = Column(types.DateTime64, nullable=False, default=str(datetime.now()))

    def getvals(self):
        temp_dict = {}
        temp_dict["wafer_loc_id"] = self.wafer_loc_id
        temp_dict["tool_name"] = self.tool_name
        temp_dict["chamber_name"] = self.chamber_name
        temp_dict["slot"] = self.slot
        temp_dict["side"] = self.side
        temp_dict["chamber_type"] = self.chamber_type
        temp_dict["alias"] = self.alias
        temp_dict["path"] = self.path
        temp_dict["hasuserecipies"] = self.hasuserecipies
        temp_dict["insertion_datetime"] = self.insertion_datetime

        return temp_dict


class SensorMetadata(Base):
    __tablename__ = 'sensor_metadata'
    __table_args__ = (
        engines.MergeTree(order_by=['sensor_name', 'wafer_loc_id']),
        {'schema': 'aix_db'},
    )
    sensor_name = Column(types.LowCardinality(String), primary_key=True)
    wafer_loc_id = Column(types.LowCardinality(String), primary_key=True)
    tool_name = Column(types.LowCardinality(String))
    alias = Column(types.LowCardinality(String))
    groups = Column(types.LowCardinality(String))
    datatype = Column(types.LowCardinality(String))
    insertion_datetime = Column(types.DateTime64, nullable=False, default=str(datetime.now()))

    def getvals(self):
        temp_dict = {}
        temp_dict["sensor_name"] = self.sensor_name
        temp_dict["wafer_loc_id"] = self.wafer_loc_id
        temp_dict["tool_name"] = self.tool_name
        temp_dict["alias"] = self.alias
        temp_dict["groups"] = self.groups
        temp_dict["datatype"] = self.datatype
        temp_dict["insertion_datetime"] = self.insertion_datetime

        return temp_dict


class AitContext(Base):
    __tablename__ = 'ait_context'
    __table_args__ = (
        engines.MergeTree(order_by=['file_id']),
        {'schema': 'aix_db'},
    )
    file_id = Column(types.LowCardinality(String), primary_key=True)
    filename = Column(types.LowCardinality(String))
    toolid = Column(types.LowCardinality(String))
    record_count = Column(types.Int64)
    size = Column(types.Int64)
    location = Column(types.LowCardinality(String))
    software = Column(types.LowCardinality(String))
    insert_ts = Column(types.DateTime64, nullable=False, default=str(datetime.now()))
    runid = Column(types.LowCardinality(String))
    chamber_name = Column(types.LowCardinality(String))
    chamber_slot = Column(types.Int64)
    chamber_side = Column(types.Int64)
    chamber_path = Column(types.LowCardinality(String))
    chamber_type = Column(types.LowCardinality(String))
    recipe_name = Column(types.LowCardinality(String))
    lot = Column(types.LowCardinality(String))
    lotcreationtime = Column(types.DateTime64, nullable=False, default=str(datetime.now()))
    wafername = Column(types.LowCardinality(String))
    wafersource_slot = Column(types.Int64)
    wafersource_foup = Column(types.LowCardinality(String))
    foupid = Column(types.LowCardinality(String))
    scribe_id = Column(types.LowCardinality(String))
    result = Column(types.LowCardinality(String))
    internalstepindex = Column(types.LowCardinality(String))
    start_time = Column(types.DateTime64, nullable=False, default=str(datetime.now()))
    calender_time = Column(types.DateTime64, nullable=False, default=str(datetime.now()))


    def getvals(self):
        temp_dict = {}
        temp_dict["file_id"] = self.file_id
        temp_dict["filename"] = self.filename
        temp_dict["toolid"] = self.toolid
        temp_dict["record_count"] = self.record_count
        temp_dict["location"] = self.location
        temp_dict["software"] = self.software
        temp_dict["insert_ts"] = self.insert_ts
        temp_dict["runid"] = self.runid
        temp_dict["chamber_name"] = self.chamber_name
        temp_dict["chamber_slot"] = self.chamber_slot
        temp_dict["chamber_side"] = self.chamber_side
        temp_dict["chamber_path"] = self.chamber_path
        temp_dict["chamber_type"] = self.chamber_type
        temp_dict["recipe_name"] = self.recipe_name
        temp_dict["lot"] = self.lot
        temp_dict["lotcreationtime"] = self.lotcreationtime
        temp_dict["wafername"] = self.wafername
        temp_dict["wafersource_slot"] = self.wafersource_slot
        temp_dict["wafersource_foup"] = self.wafersource_foup
        temp_dict["foupid"] = self.foupid
        temp_dict["scribe_id"] = self.scribe_id
        temp_dict["result"] = self.result
        temp_dict["internalstepindex"] = self.internalstepindex
        temp_dict["start_time"] = self.start_time
        temp_dict["calender_time"] = self.calender_time

        return temp_dict


class AixTest(Base):
    __tablename__ = 'aix_test'
    __table_args__ = (
        engines.MergeTree(order_by=['file_id']),
        {'schema': 'aix_db'},
    )
    file_id = Column(types.String, primary_key=True)
    foupid = Column(types.String)
    scribe_id =  Column(types.String)
    result =  Column(types.String)
    internalstepindex = Column(types.String)
    start_time = Column(types.DateTime64, nullable=False, default=datetime.now())
    calender_time = Column(types.DateTime64, nullable=False, default=datetime.now())

class Step(Base):
    __tablename__ = 'step'
    __table_args__ = (
        engines.MergeTree(order_by=['file_id']),
        {'schema': 'aix_db'},
    )
    file_id = Column(types.LowCardinality(String), primary_key=True)
    step = Column(types.LowCardinality(String))
    loop = Column(types.LowCardinality(String))


class Alarm(Base):
    __tablename__ = 'alarms_data'
    __table_args__ = (
        engines.MergeTree(order_by=['unique_id']),
        {'schema': 'aix_db'},
    )
    unique_id = Column(types.LowCardinality(String), primary_key=True)
    toolid = Column(types.LowCardinality(String))
    toolname = Column(types.LowCardinality(String))
    timestamp = Column(types.LowCardinality(String))
    alarmid = Column(types.LowCardinality(String))
    alarmtext = Column(types.LowCardinality(String))
    severity = Column(types.LowCardinality(String))
    recipename = Column(types.LowCardinality(String))
    lotid = Column(types.LowCardinality(String))
    waferid = Column(types.LowCardinality(String))
    slotnumber = Column(types.LowCardinality(String))
    run = Column(types.LowCardinality(String))
    runstarttime = Column(types.LowCardinality(String))
    partitionid = Column(types.LowCardinality(String))
    source = Column(types.LowCardinality(String))
    notifyapp = Column(types.LowCardinality(String))
    notifyuser = Column(types.LowCardinality(String))
    gem_id = Column(types.LowCardinality(String))

    def getvals(self):
        temp_dict = {}
        temp_dict["toolname"] = self.toolname
        temp_dict["timestamp"] = self.timestamp
        temp_dict["alarmid"] = self.alarmid
        temp_dict["alarmtext"] = self.alarmtext
        temp_dict["severity"] = self.severity
        temp_dict["run"] = self.run

        return temp_dict

class ContextMetadata(Base):
    __tablename__ = 'context_meta_data_new'
    __table_args__ = (
        engines.MergeTree(order_by=['file_id']),
        {'schema': 'aix_db'},
    )
    file_id=Column(types.LowCardinality(String), primary_key=True)
    record_count=Column(types.LowCardinality(String))
    file_size=Column(types.LowCardinality(String))
    location=Column(types.LowCardinality(String))
    software=Column(types.LowCardinality(String))
    insert_ts=Column(types.LowCardinality(String))
    toolid=Column(types.LowCardinality(String))
    runid=Column(types.LowCardinality(String))
    chamber_name=Column(types.LowCardinality(String))
    chamber_slot=Column(types.LowCardinality(String))
    chamber_side=Column(types.LowCardinality(String))
    chamber_path=Column(types.LowCardinality(String))
    chamber_type=Column(types.LowCardinality(String))
    recipe_name=Column(types.LowCardinality(String))
    lot=Column(types.LowCardinality(String))
    lotcreationtime=Column(types.LowCardinality(String))
    wafername=Column(types.LowCardinality(String))
    wafersource_slot=Column(types.LowCardinality(String))
    wafersource_foup=Column(types.LowCardinality(String))
    foupid=Column(types.LowCardinality(String))
    scribe_id=Column(types.LowCardinality(String))
    result=Column(types.LowCardinality(String))
    step=Column(types.LowCardinality(String))
    stepname=Column(types.LowCardinality(String))
    recipetype=Column(types.LowCardinality(String))
    internalstepindex=Column(types.LowCardinality(String))
    starttime=Column(types.LowCardinality(String))
    calendartime=Column(types.LowCardinality(String))
    filename=Column(types.LowCardinality(String))


    def getvals(self):
        temp_dict = {}
        temp_dict["file_id"]=self.file_id
        temp_dict["record_count"]=self.record_count
        temp_dict["file_size"]=self.file_size
        temp_dict["location"]=self.location
        temp_dict["software"]=self.software
        temp_dict["insert_ts"]=self.insert_ts
        temp_dict["toolid"]=self.toolid
        temp_dict["runid"]=self.runid
        temp_dict["chamber_name"]=self.chamber_name
        temp_dict["chamber_slot"]=self.chamber_slot
        temp_dict["chamber_side"]=self.chamber_side
        temp_dict["chamber_path"]=self.chamber_path
        temp_dict["chamber_type"]=self.chamber_type
        temp_dict["recipe_name"]=self.recipe_name
        temp_dict["lot"]=self.lot
        temp_dict["lotcreationtime"]=self.lotcreationtime
        temp_dict["wafername"]=self.wafername
        temp_dict["wafersource_slot"]=self.wafersource_slot
        temp_dict["wafersource_foup"]=self.wafersource_foup
        temp_dict["foupid"]=self.foupid
        temp_dict["scribe_id"]=self.scribe_id
        temp_dict["result"]=self.result
        temp_dict["step"]=self.step
        temp_dict["stepname"]=self.stepname
        temp_dict["recipetype"]=self.stepname
        temp_dict["internalstepindex"]=self.stepname
        temp_dict["starttime"]=self.stepname
        temp_dict["calendartime"]=self.stepname
        temp_dict["filename"]=self.stepname



        return temp_dict



#ToolMetadata.__table__.create(engine)
#ChamberMetadata.__table__.create(engine)
#SensorMetadata.__table__.create(engine)
Base.metadata.create_all(bind=DbConnectionUtil().get_database_engine(), checkfirst=True)