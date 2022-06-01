from typing import Tuple

import bcrypt
import jwt
from jwt import encode

from dashboard_analytics.exceptions.security_exception import UnauthorizedException
from dashboard_analytics.logger.logger import get_aix_ms_logger as AIXLogger
from dashboard_analytics.settings.constants import Constants
from dashboard_analytics.settings.env_vars import EnvironmentVars

constant = Constants()
env_vars = EnvironmentVars()


class AuthUtil:
    def __init__(self):
        self.logger = AIXLogger(self.constants.APP_NAME)

    @staticmethod
    def is_pswd_valid(user_pswd, persisted_pswd):
        return bcrypt.checkpw(user_pswd.encode(), persisted_pswd.encode())

    @staticmethod
    def salt_and_hash_pswd(pswd):
        salt = bcrypt.gensalt()
        hashed_password = bcrypt.hashpw(pswd.encode(), salt).decode()
        return hashed_password

    def generate_jwt_token( jwt_attributes):
        encoded_jwt = encode(
            jwt_attributes, env_vars.JWT_SECRET_KEY, algorithm="HS256"
        )

        return encoded_jwt

    @staticmethod
    def get_current_user(token):
        try:
            payload = jwt.decode(
                token,
                env_vars.JWT_SECRET_KEY,
                algorithms=["HS256"],
                options={
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_nbf": False,
                    "verify_iat": True,
                    "verify_aud": False,
                },
            )
            return payload
        except Exception as e:
            raise UnauthorizedException

    @staticmethod
    def get_authorization_scheme_param(
        authorization_header_value: str,
    ) -> Tuple[str, str]:
        if not authorization_header_value:
            return "", ""
        scheme, _, param = authorization_header_value.partition(" ")
        return scheme, param
