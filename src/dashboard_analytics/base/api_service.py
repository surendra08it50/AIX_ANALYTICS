import logging
from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from dashboard_analytics.auth.auth_util import AuthUtil
from dashboard_analytics.base.util import UtilBase
from dashboard_analytics.exceptions.db import DbException
from dashboard_analytics.exceptions.security_exception import (
    ForbiddenException,
    UnauthorizedException,
)
from dashboard_analytics.logger.logger import get_log_handler


class FastService(UtilBase):
    def __init__(self, name=None, version=None, dev=False):
        super().__init__(dev=dev)
        self.app = FastAPI()
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        self.name = name
        self.version = version

        self._set_uvicornlog()
        self._set_middleware()

    def _set_uvicornlog(self):
        self.uvicornlog = logging.getLogger("uvicorn")
        self.uvicornlog.setLevel(level=self.env_vars.SQALCHEMY_LOG_LEVEL)
        self.uvicornlog.addHandler(get_log_handler(self.env_vars.APP_NAME))

    def _set_middleware(self):
        @self.app.exception_handler(Exception)
        def unicorn_exception_handler(request: Request, exc: Exception):
            status_code = 500
            if type(exc) is DbException:
                status_code = 400

            if type(exc) is UnauthorizedException:
                status_code = 401

            if type(exc) is ForbiddenException:
                status_code = 403

            self.logger.error(f"{str(exc) or exc.message}")

            return JSONResponse(
                status_code=status_code,
                content={"message": f"{str(exc) or exc.message}"},
            )

        @self.app.get("/", tags=["General"])
        async def home(request: Request):
            """Health check."""
            response = {
                "status-code": HTTPStatus.OK,
                "name": self.name,
                "version": self.version,
            }
            return response

        @self.app.middleware("http")
        async def validate_authenticity(request: Request, call_next):
            if not self.dev:
                if eval(self.env_vars.JWT_ENABLED) and not (
                    request["path"] in self.constants.JWT_EXCLUDED_PATH
                ):
                    authorization: str = request.headers.get("Authorization")
                    scheme, param = AuthUtil.get_authorization_scheme_param(
                        authorization
                    )
                    if not authorization or scheme.lower() != "bearer":
                        self.logger.error("No bearer token found")
                        raise UnauthorizedException
                    payload = AuthUtil.get_current_user(param)

                    # Admin Routes check
                    if (
                        request.url.path in self.constants.ADMIN_ROUTES
                        and payload.get("roles") != "admin"
                    ):
                        self.logger.error("Requester is not an admin")
                        raise ForbiddenException
                    request.state.security_context = payload
            return await call_next(request)
