from datetime import datetime

from sqlalchemy import and_, exc, func
from sqlalchemy.orm import load_only

from dashboard_analytics.base.clickhouse_db_service import ServiceBase
from dashboard_analytics.base.clickhouse_models import (
    ToolMetadata,
    ChamberMetadata,
    SensorMetadata,
    AixTest,
    AitContext,
    Step,
    Alarm,
    ContextMetadata
)
from dashboard_analytics.exceptions.db import DbException


class ToolMetadataService(ServiceBase):
    def __init__(self):
        super().__init__(ToolMetadata)

    def get_tools(self):
        return super().find_all()
    
    def get_count(self, findManyOptions=None):
        try:
            if findManyOptions is not None:
                results = self.session.query(ToolMetadata).filter(findManyOptions).count()
            else:
                results = self.session.query(ToolMetadata).count()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()
    
    def get_distinct_count(self,fields, findManyOptions=None):
        try:
            if findManyOptions is not None:
                results = self.session.query(ToolMetadata).filter(findManyOptions).options(load_only(*fields)).distinct().count()
            else:
                results = self.session.query(ToolMetadata).options(load_only(*fields)).distinct().count()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()
    
    def exist_tool(self, newTool):
        find_one_options = ToolMetadata.tool_name == newTool.tool_name
        result = self.get_tool_by_toolname(find_one_options)

        return result

    def get_tool_by_toolname(self, find_options):
        return super().find_one(find_options)


class ChamberMetadataService(ServiceBase):
    def __init__(self):
        super().__init__(ChamberMetadata)

    def get_count(self, findManyOptions=None):
        try:
            if findManyOptions is not None:
                results = self.session.query(ChamberMetadata).filter(findManyOptions).count()
            else:
                results = self.session.query(ChamberMetadata).count()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()
    
    def get_distinct_count(self,fields,findManyOptions=None):
        try:
            if findManyOptions is not None:
                results = self.session.query(ChamberMetadata).filter(findManyOptions).options(load_only(*fields)).distinct().count()
            else:
                results = self.session.query(ChamberMetadata).options(load_only(*fields)).distinct().count()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

        
    def get_chambers(self, filter_options=None, offset = 1, limit=1):

        # return super().find_all_ab(filter_options, offset = offset, limit=limit)
        try:
            results = self.session.query(ChamberMetadata).all()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def exist_tool(self, newChamber):
        find_one_options = ChamberMetadata.chamber_name == newChamber.chamber_name
        result = self.__get_tool_by_toolname(find_one_options)

        return result
    
    def get_chambers_with_pagination(self, findManyOptions=None, offset=1, limit=1):
        try:
            results = self.session.query(ChamberMetadata).filter(findManyOptions).offset(offset).limit(limit).all()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()
    

class SensorMetadataService(ServiceBase):
    def __init__(self):
        super().__init__(SensorMetadata)
    
    def get_count(self,findManyOptions=None):
        try:
            if findManyOptions is not None:
                results = self.session.query(SensorMetadata).filter(findManyOptions).count()
            else:
                results = self.session.query(SensorMetadata).count()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()
    
    def get_distinct_count(self,fields,findManyOptions=None):
        try:
            if findManyOptions is not None:
                results = self.session.query(SensorMetadata).filter(findManyOptions).options(load_only(*fields)).distinct().count()
            else:
                results = self.session.query(SensorMetadata).options(load_only(*fields)).distinct().count()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def get_sensors(self, filter_options=None,offset = None, limit=None):

        return super().find_all_ab(filter_options, offset = offset, limit=limit)
    
    def get_sensors_with_pagination(self, findManyOptions=None, offset=1, limit=1):
        try:
            results = self.session.query(SensorMetadata).filter(findManyOptions).offset(offset).limit(limit).all()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()


class AixTestService(ServiceBase):
    def __init__(self):
        super().__init__(AixTest)

    def get_aixtest_datas(self):
        return super().find_all()


class AitContextService(ServiceBase):
    def __init__(self):
        super().__init__(AitContext)

    def get_context_datas(self):
        return super().find_all()

    def get_count(self,findManyOptions=None):
        try:
            if findManyOptions is not None:
                results = self.session.query(AitContext).filter(findManyOptions).count()
            else:
                results = self.session.query(AitContext).count()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()
    
    def get_distinct_count(self,fields,findManyOptions=None):
        try:
            if findManyOptions is not None:
                results = self.session.query(AitContext).filter(findManyOptions).options(load_only(*fields)).count()
            else:
                results = self.session.query(AitContext).options(load_only(*fields)).distinct().count()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def get_context_data_with_pagination(self, findManyOptions=None, offset=1, limit=1):
        try:
            results = self.session.query(AitContext).filter(findManyOptions).offset(offset).limit(limit).all()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()
    
    def get_context_with_filter(self, findManyOptions):
        try:
            results = self.session.query(AitContext).filter(findManyOptions).all()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()


class AlarmService(ServiceBase):
    def __init__(self):
        super().__init__(Alarm)

    def get_count(self,findManyOptions=None):
        try:
            if findManyOptions is not None:
                results = self.session.query(Alarm).filter(findManyOptions).count()
            else:
                results = self.session.query(Alarm).count()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()
    
    def get_distinct_count(self,fields,findManyOptions=None):
        try:
            if findManyOptions is not None:
                results = self.session.query(Alarm).filter(findManyOptions).options(load_only(*fields)).distinct().count()
            else:
                results = self.session.query(Alarm).options(load_only(*fields)).distinct().count()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

    def get_alarm_data_with_pagination(self, findManyOptions=None, offset=1, limit=1):
        try:
            results = self.session.query(Alarm).offset(offset).limit(limit).all()
            return results
        except exc.SQLAlchemyError as e:
            raise DbException(e)
        except Exception as e:
            raise e
        finally:
            self.session.close()

class StepService(ServiceBase):
    def __init__(self):
        super().__init__(Step)

    def get_step_datas(self):
        return super().find_all()


class ContextMetadataService(ServiceBase):
    def __init__(self):
        super().__init__(ContextMetadata)

    def get_tools(self):
        return super().find_all()        