# Path settings

import os

TO_BE_FILLED = "xxxx"


class Constants(object):
    def __init__(self):
        # TABLE NAMES
        self.USER_TABLE_NM = "aix_users"
        self.APP_LIST_MASTER_TABLE_NM = "app_list_master"
        self.USER_ROLE_MASTER_TABLE_NM = "user_role_master"
        self.USER_PREFERENCES_TABLE_NM = "user_preference"
        self.SOFTWARE_TABLE_NM = "software_master"
        self.TOOL_TABLE_NM = "tool_master"
        self.CHAMBER_TABLE_NM = "chamber_master"
        self.SOFTWARE_META_TABLE_NM = "software_metadata"
        self.TOOL_SOFTWARE_MAP_TABLE_NM = "tool_software_mapping"
        self.TOOL_SOFTWARE_MONITOR_TABLE_NM = "tool_software_monitoring"
        self.RESOURCE_STATS_TABLE_NM = "resource_stats"

        self.JWT_EXCLUDED_PATH = ["/userapi/auth/user", "/auth/user"]
        self.JWT_EXPIRATION_TIME = os.environ.get("JWT_EXPIRATION_TIME", 60)  # minutes


        # ADMIN_ROUTES
        self.ADMIN_ROUTES = ["/userapi/user", "/user"]

        # SMS Docker Registery
        self.SMS_DOCKER_REGISTRY = "10.41.31.187:5000"
