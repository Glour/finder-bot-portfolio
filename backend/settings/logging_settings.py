import logging
import typing as tp
from enum import Enum

import betterlogging as bl
from pydantic import BaseModel, ConfigDict, Field

log_level = logging.INFO
bl.basic_colorized_config(level=log_level)

logging.basicConfig(
    level=log_level,
    format="%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)


class LoggerLevelType(str, Enum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"


class LoggingSettings(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    fmt: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    default_handlers: list[str] = ["console"]
    level: LoggerLevelType = Field(alias="logging_level", default=LoggerLevelType.DEBUG)

    @property
    def config(
            self,
    ) -> dict[str, tp.Any,]:
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "verbose": {"format": self.fmt},
                "default": {
                    "()": "uvicorn.logging.DefaultFormatter",
                    "fmt": "%(levelprefix)s %(message)s",
                    "use_colors": None,
                },
                "access": {
                    "()": "uvicorn.logging.AccessFormatter",
                    "fmt": "%(levelprefix)s %(client_addr)s - '%(request_line)s' %(status_code)s",  # noqa: E501
                },
            },
            "handlers": {
                "console": {
                    "level": "DEBUG",
                    "class": "logging.StreamHandler",
                    "formatter": "verbose",
                },
                "default": {
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                },
                "access": {
                    "formatter": "access",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                },
            },
            "loggers": {
                "": {
                    "handlers": self.default_handlers,
                    "level": "INFO",
                },
                "uvicorn.error": {
                    "level": "INFO",
                },
                "uvicorn.access": {
                    "handlers": ["access"],
                    "level": "INFO",
                    "propagate": False,
                },
            },
            "root": {
                "level": self.level,
                "formatter": "verbose",
                "handlers": self.default_handlers,
            },
        }
