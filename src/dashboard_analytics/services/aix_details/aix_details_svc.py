import math
from datetime import datetime
from http import HTTPStatus
from fastapi import responses
import numpy as np
import pandas as pd

from fastapi import Request
from fastapi.responses import RedirectResponse
from dashboard_analytics.commons.models import (
    InputItemAixDetails,
    InputItemAixGraph,
    InputItemToolInfo,
    InputItemRecordCount
)
from dashboard_analytics.base.api_service import FastService
from dashboard_analytics.svc.software_mgmt_services import (
    ToolMetadataService,
    ChamberMetadataService,
    SensorMetadataService,
    AixTestService,
    AitContextService,
    StepService,
    AlarmService,
    ContextMetadataService
)
from dashboard_analytics.base.clickhouse_models import (
    ToolMetadata,
    ChamberMetadata,
    SensorMetadata,
    AitContext,
    Alarm,
    ContextMetadata
)
from dashboard_analytics.settings.constants import Constants
from sqlalchemy import and_
from sqlalchemy import cast, Date

#from dashboard_analytics.base.clickhouse_models import Alarm


class GetAixDetailsService(FastService):
    def __init__(self):
        super().__init__()
        self._routes()
        self.constants = Constants()
    
    def get_toolmetadata_records(self):
        '''
        get records from ToolMetadataService
        return: dataframe
        '''
        tool = ToolMetadataService()
        tool_records = tool.get_tools()

        dict_updated = []
        for rec in tool_records:
            tmp_dict = rec.__dict__
            tmp_dict.pop('_sa_instance_state')
            dict_updated.append(tmp_dict)
        
        if len(dict_updated)==0:
            tab_columns = ['tool_name', 'timezone_microsoft', 'software_version', 
                    'alias', 'timezone_iana', 'tool_type']
            df = pd.DataFrame(columns=tab_columns, dtype=object)
        else:
            df = pd.DataFrame([rec for rec in dict_updated])

        return df
    
    def get_chambermetadata_records(self):
        '''
        get records from ChamberMetadataService
        return: dataframe
        '''
        chamber = ChamberMetadataService()
        chamber_records = chamber.get_chambers()
        

        dict_updated = []
        for rec in chamber_records:
            tmp_dict = rec.__dict__
            tmp_dict.pop('_sa_instance_state')
            dict_updated.append(tmp_dict)
        
        if len(dict_updated)==0:
            tab_columns = ['slot', 'tool_name', 'hasuserecipies', 'alias', 'side',
                         'chamber_name', 'wafer_loc_id', 'path', 'chamber_type']
            df = pd.DataFrame(columns=tab_columns, dtype=object)
        else:
            df = pd.DataFrame([rec for rec in dict_updated])

        return df
    
    def get_sensormetadata_records(self):
        '''
        get records from SensorMetadataService
        return: dataframe
        '''
        sensor = SensorMetadataService()
        sensor_records = sensor.get_sensors()

        dict_updated = []
        for rec in sensor_records:
            tmp_dict = rec.__dict__
            tmp_dict.pop('_sa_instance_state')
            dict_updated.append(tmp_dict)
        
        if len(dict_updated)==0:
            tab_columns = ['datatype', 'alias', 
                        'wafer_loc_id', 'groups', 'tool_name', 'sensor_name']
            df = pd.DataFrame(columns=tab_columns, dtype=object)
        else:
            df = pd.DataFrame([rec for rec in dict_updated])

        return df
    
    def get_aixtest_records(self):
        '''
        get records from AixTestService
        return: dataframe
        '''
        aixtest = AixTestService()
        aixtest_records = aixtest.get_aixtest_datas()

        dict_updated = []
        for rec in aixtest_records:
            tmp_dict = rec.__dict__
            tmp_dict.pop('_sa_instance_state')
            dict_updated.append(tmp_dict)
        
        if len(dict_updated)==0:
            tab_columns = ['file_id', 'foupid', 'scribe_id', 'result',
                         'internalstepindex', 'start_time', 'calender_time']
            df = pd.DataFrame(columns=tab_columns, dtype=object)
        else:
            df = pd.DataFrame([rec for rec in dict_updated])

        return df
    
    def get_ait_context_records(self, context_records=None):
        '''
        get records from AitContext
        return: dataframe
        '''
        if context_records is None:
            ait_context = AitContextService()
            context_records = ait_context.get_context_datas()

        dict_updated = []
        for rec in context_records:
            tmp_dict = rec.__dict__
            tmp_dict.pop('_sa_instance_state')
            dict_updated.append(tmp_dict)
        
        if len(dict_updated)==0:
            tab_columns = ['file_id', 'filename', 'toolid', 'record_count', 'size', 
                    'location', 'software', 'insert_ts', 'runid', 'chamber_name', 
                    'chamber_slot', 'chamber_side', 'chamber_path', 'chamber_type', 
                    'recipe_name', 'lot', 'lotcreationtime', 'wafername', 'wafersource_slot', 
                    'wafersource_foup', 'foupid', 'scribe_id', 'result', 'internalstepindex', 
                    'start_time', 'calender_time']
            df = pd.DataFrame(columns=tab_columns, dtype=object)
        else:
            df = pd.DataFrame([rec for rec in dict_updated])

        return df
    
    def filter_ait_context_records(self, context_records, start_date, end_date):
        '''
        get records from AitContext
        return: dataframe
        '''

        df = self.get_ait_context_records(context_records)
        df['start_time'] = pd.to_datetime( df['start_time'])
        df['date'] = df['start_time'].dt.date
        
        df['year'] = df['start_time'].dt.year
        df['month'] = df['start_time'].dt.month
        df['day'] = df['start_time'].dt.day
        year_dict = df.groupby(['year', ])['filename'].agg(['count']).reset_index().to_dict(orient='records')
        month_dict = df.groupby(['year', 'month'])['filename'].agg(['count']).reset_index()
        day_dict = df.groupby(['year', 'month', 'day'])['filename'].agg(['count']).reset_index()
        day_dict.columns = ["year",	"month",'day_no','total']
        response = {}
        for year_data in year_dict:
            year = year_data['year']
            response[year] = {
                "total": year_data["count"],
                "month":[]
            }
            for i, month_row in month_dict[month_dict['year']==year].iterrows():
                month_result = {}
                month = month_row['month']
                month_result['month_no'] = int(month)
                month_result['total'] = int(month_row['count'])
                month_result['day'] = day_dict[(day_dict['year']==year) & (day_dict['month']==month)][['day_no','total']].to_dict(orient='records')
                response[year]["month"].append(month_result)
        return response

    
    def get_contextmetadata_records(self):
        '''
        get records from contextMetadataService
        return: dataframe
        '''
        tool = ContextMetadataService()
        tool_records = tool.get_tools()
        dict_updated = []
        for rec in tool_records:
            tmp_dict = rec.__dict__
            tmp_dict.pop('_sa_instance_state')
            dict_updated.append(tmp_dict)
        
        if len(dict_updated)==0:
            tab_columns = ['file_id', 'record_count', 'file_size']
            df = pd.DataFrame(columns=tab_columns, dtype=object)
        else:
            df = pd.DataFrame([rec for rec in dict_updated])

        # df=df.loc[0:5, :].copy()

        total_tools = df['toolid'].nunique()
        total_chambers = df['chamber_name'].nunique()
        total_recipes = df['recipe_name'].nunique()
        total_files = df['file_id'].nunique()
        latest_insertion_time = df['insert_ts'].max()
        latest_start_time = df['starttime'].max()
        return {
                "Status": HTTPStatus.OK,
                "data": {
                   "total_tools":total_tools,
                    "total_chambers":total_chambers,
                    "total_recipes": total_recipes,
                    "total_files": total_files,
                    "latest_insertion_time": latest_insertion_time,
                    "latest_start_time": latest_start_time
                    
                    }
                }


    def _routes(self):
        @self.app.post("/analytics/get_aix_details/", tags=["aix_details"])
        async def insert_tool_chamber(item: InputItemAixDetails):
            """rest endpoint for get_aix_details/"""
            start_time = datetime.today()
            endpoint = "get_aix_details"
            self.logger.info(f"input item : {item}")

            toolmeta_df = self.get_toolmetadata_records()
            chambermeta_df = self.get_chambermetadata_records()
            #sensormeta_df = self.get_sensormetadata_records()
            #aixtest_df = self.get_aixtest_records()

            if len(item.tools) != 0:
                filter_options = and_(item.start_date.strftime("%Y-%m-%d") <= AitContext.start_time.cast(Date),
                    AitContext.start_time.cast(Date) <= item.end_date.strftime("%Y-%m-%d"),
                    AitContext.toolid.in_(item.tools))
            else:
                filter_options = and_(item.start_date.strftime("%Y-%m-%d") <= AitContext.start_time.cast(Date),
                    AitContext.start_time.cast(Date) <= item.end_date.strftime("%Y-%m-%d"))
            
            ait_context = AitContextService()
            ait_context_records = ait_context.get_context_with_filter(filter_options)
            aitcontext_df = self.get_ait_context_records(ait_context_records)

            self.logger.info(f"got get_toolmetadata records : {toolmeta_df.shape[0]}")
            self.logger.info(f"got get_chambermetadata records : {chambermeta_df.shape[0]}")
            #self.logger.info(f"got get_sensormetadata records : {sensormeta_df.shape[0]}")
            #self.logger.info(f"got get_aixtest records : {aixtest_df.shape[0]}")
            self.logger.info(f"got get_ait_context records : {aitcontext_df.shape[0]}")

            # Commented out to get the tool and chamber data from AIT Context
            # total_tools = toolmeta_df['tool_name'].nunique()
            # total_chambers = chambermeta_df['chamber_name'].nunique()

            total_tools = aitcontext_df['toolid'].nunique()
            total_chambers = aitcontext_df['chamber_name'].nunique()
            total_recipes = aitcontext_df['recipe_name'].nunique()
            total_files = aitcontext_df['file_id'].nunique()

            tool_name_agg_file_cnt = aitcontext_df.groupby('toolid')['file_id'].nunique().reset_index().rename(columns={'file_id':'total_files'})
            tool_name_agg_file_cnt_dict = tool_name_agg_file_cnt.to_dict('records')
            tool_name_agg_max_date = aitcontext_df.groupby('toolid').insert_ts.max().reset_index().rename(columns={'insert_ts':'latest_file'})
            tool_name_agg_max_date_dict = tool_name_agg_max_date.to_dict('records')
            tool_name_agg_software_cnt = aitcontext_df.groupby('toolid')['software'].nunique().reset_index().rename(columns={'software':'total_softwares'})
            tool_name_agg_software_cnt_dict = tool_name_agg_software_cnt.to_dict('records')

            total_details = []
            if len(tool_name_agg_file_cnt_dict)!=0:
                df_tmp1 = pd.DataFrame(tool_name_agg_file_cnt_dict)
                df_tmp2 = pd.DataFrame(tool_name_agg_max_date_dict)
                df_tmp3 = pd.DataFrame(tool_name_agg_software_cnt_dict)
                df_tmp4 = pd.merge(df_tmp1, df_tmp2, how="inner", on=['toolid'])                                                     
                df_tmp5 = pd.merge(df_tmp4, df_tmp3, how="inner", on=['toolid'])
                df_tmp5['alert'] = 1
                total_details.extend(df_tmp5.to_dict('records'))

            # log to db
            #self.db_logger.info(
            #    "db logging.",
            #    extra={"endpoint": endpoint, "datetime": start_time, "status": 200},
            #)

            return {
                "Status": HTTPStatus.OK,
                "data": {
                   "total_tools":total_tools,
                    "total_chambers":total_chambers,
                    "total_recipes": total_recipes,
                    "total_files": total_files,
                    "tool_details":total_details
                    }
                }
        
        
        @self.app.post("/analytics/get_aix_graph/", tags=["aix_graph"])
        async def get_aix_graph(item: InputItemAixGraph):
            """rest endpoint for get_aix_graph/"""
            start_time = datetime.today()
            endpoint = "get_aix_graph"

            if len(item.tools) != 0:
                filter_options = and_(item.start_date.strftime("%Y-%m-%d") <= AitContext.start_time.cast(Date),
                    AitContext.start_time.cast(Date) <= item.end_date.strftime("%Y-%m-%d"),
                    AitContext.toolid.in_(item.tools))
            else:
                filter_options = and_(item.start_date.strftime("%Y-%m-%d") <= AitContext.start_time.cast(Date),
                    AitContext.start_time.cast(Date) <= item.end_date.strftime("%Y-%m-%d"))
            #print(f"filter_query : {filter_options} type of AitContext.start_time {type(AitContext.start_time)} item.start_date.isoformat() :{type(item.start_date)}")

            ait_context = AitContextService()
            ait_context_records = ait_context.get_context_with_filter(filter_options)
            self.logger.info(f"input item : {item}")
            result_dict = self.filter_ait_context_records(ait_context_records, item.start_date , item.end_date)

            return {
               "Status": HTTPStatus.OK,
               "data": result_dict
            }
        

        @self.app.post("/analytics/tool_info/", tags=["tool_info"])
        async def get_tool_info(item: InputItemToolInfo):
            tool_meta_service = ToolMetadataService()
            find_one_options = ToolMetadata.tool_name == item.tools
            tool_data = tool_meta_service.get_tool_by_toolname(find_one_options)

            if tool_data is None:
                response = {
                    "Status": HTTPStatus.OK,
                    "data": {}
                    }
            else:
                response = {
                    "Status": HTTPStatus.OK,
                    "data": tool_data.getvals()
                    }

            return response
        
        
        @self.app.post("/analytics/chamber_info/", tags=["chamber_info"])
        async def get_chamber_info(item: InputItemToolInfo):
            chamber_meta_service = ChamberMetadataService()
            filter_options = ChamberMetadata.tool_name == item.tools
            total_count = chamber_meta_service.get_count(filter_options)
            if item.page:
                offset = (item.page - 1) * item.limit
                chmabers = chamber_meta_service.get_chambers_with_pagination(filter_options, offset, item.limit)
            else:
                chmabers = chamber_meta_service.get_chambers_with_pagination(filter_options, None, None)

            response = {
                "Status": HTTPStatus.OK,
                "total_count":total_count,
                "data":[chamber_data.getvals() for chamber_data in chmabers]
            }

            return response
        

        @self.app.post("/analytics/sensor_info/", tags=["sensor_info"])
        async def get_sensor_info(item: InputItemToolInfo):
            sensor_meta_service = SensorMetadataService()
            filter_options = SensorMetadata.tool_name == item.tools
            total_count = sensor_meta_service.get_count(filter_options)
            if item.page:
                offset = (item.page - 1) * item.limit
                sensors = sensor_meta_service.get_sensors_with_pagination(filter_options, offset, item.limit)
            else:
                sensors = sensor_meta_service.get_sensors_with_pagination(filter_options, None, None)

            response = {
                "Status": HTTPStatus.OK,
                "total_count":total_count,
                "data":[sensor_data.getvals() for sensor_data in sensors]
            }

            return response

        @self.app.post("/analytics/file_info/", tags=["file_info"])
        async def get_file_info(item: InputItemToolInfo):
            ait_context = AitContextService()
            filter_options = AitContext.toolid == item.tools
            total_count = ait_context.get_count(filter_options)
            if item.page:
                offset = (item.page - 1) * item.limit
                ait_context_records = ait_context.get_context_data_with_pagination(filter_options, offset, item.limit)
            else:
                ait_context_records = ait_context.get_context_data_with_pagination(filter_options, None, None)
            self.logger.info(f"input item : {item}")

            return {
               "Status": HTTPStatus.OK,
               "total_count":total_count,
               "data":[context_data.getvals() for context_data in ait_context_records]
            }

        @self.app.post("/analytics/alarm_info/", tags=["alrm_info"])
        async def get_alarm_info(item: InputItemToolInfo):
            alarm_service = AlarmService()
            filter_options = Alarm.toolname == item.tools
            total_count = alarm_service.get_count(filter_options)
            if item.page:
                offset = (item.page - 1) * item.limit
                alarm_records = alarm_service.get_alarm_data_with_pagination(filter_options, offset, item.limit)
            else:
                alarm_records = alarm_service.get_alarm_data_with_pagination(filter_options, None, None)
            self.logger.info(f"input item : {item}")

            return {
               "Status": HTTPStatus.OK,
               "total_count":total_count,
               "data":[alarm_data.getvals() for alarm_data in alarm_records]
            }
        

        @self.app.post("/analytics/record_count/", tags=["record_count"])
        async def get_record_count_info(item: InputItemRecordCount):
            self.logger.info(f"input item : {item}")

            #toolmeta_service = ToolMetadataService()
            #filter_options = and_(item.start_date.strftime("%Y-%m-%d") <= ToolMetadata.insertion_datetime.cast(Date),
            #    ToolMetadata.insertion_datetime.cast(Date) <= item.end_date.strftime("%Y-%m-%d"),
            #    ToolMetadata.tool_name == item.tools)
            #unq_tool_names = toolmeta_service.get_distinct_count(['tool_name'],filter_options)

            chambermeta_service = ChamberMetadataService()
            filter_options = and_(item.start_date.strftime("%Y-%m-%d") <= ChamberMetadata.insertion_datetime.cast(Date),
                ChamberMetadata.insertion_datetime.cast(Date) <= item.end_date.strftime("%Y-%m-%d"),
                ChamberMetadata.tool_name == item.tools)
            unq_chamber_names = chambermeta_service.get_distinct_count(['chamber_name'],filter_options)

            sensormeta_service = SensorMetadataService()
            filter_options = and_(item.start_date.strftime("%Y-%m-%d") <= SensorMetadata.insertion_datetime.cast(Date),
                SensorMetadata.insertion_datetime.cast(Date) <= item.end_date.strftime("%Y-%m-%d"),
                SensorMetadata.tool_name == item.tools)
            unq_sensor_names = sensormeta_service.get_distinct_count(['sensor_name'],filter_options)

            aitcontext_service = AitContextService()
            filter_options = and_(item.start_date.strftime("%Y-%m-%d") <= AitContext.start_time.cast(Date),
                AitContext.start_time.cast(Date) <= item.end_date.strftime("%Y-%m-%d"),
                AitContext.toolid == item.tools)
            unq_file_ids = aitcontext_service.get_distinct_count(['file_id'],filter_options)

            alarm_service = AlarmService()
            filter_options = and_(#item.start_date.strftime("%Y-%m-%d") <= Alarm.timestamp.cast(Date),
                #Alarm.timestamp.cast(Date) <= item.end_date.strftime("%Y-%m-%d"),
                Alarm.toolname == item.tools)
            unq_alarmids = alarm_service.get_distinct_count(['alarmid'],filter_options)

            response_data = {"total_chambers":unq_chamber_names,
                        "total_sensors":unq_sensor_names,
                        "total_files":unq_file_ids,
                        "total_alarm_ids":unq_alarmids
                        }
            self.logger.info(f"response item : {response_data}")

            return {
               "Status": HTTPStatus.OK,
               "data":response_data
            }

      
        @self.app.get("/analytics/context_meta_data/", tags=["context_meta_data"])
        async def get_context_meta_data():
            
            toolmeta_dict = self.get_contextmetadata_records()
            return toolmeta_dict



client = GetAixDetailsService()
app = client.app
