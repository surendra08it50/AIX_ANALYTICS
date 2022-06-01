from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional

from pydantic import BaseModel

# get_aix_details
class InputItemAixDetails(BaseModel):
    start_date: date
    end_date: date
    tools: List[str]


# get_aix_graph
class InputItemAixGraph(BaseModel):
    start_date: date
    end_date: date
    tools: List[str]

# tool/sensor/chamber info
class InputItemToolInfo(BaseModel):
    tools: str
    limit: Optional[int]=10
    page: Optional[int]=1


# get_record_count
class InputItemRecordCount(BaseModel):
    start_date: date
    end_date: date
    tools: str

