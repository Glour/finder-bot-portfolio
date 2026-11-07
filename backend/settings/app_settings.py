from functools import lru_cache

from pydantic import Field
from pydantic_settings import SettingsConfigDict, BaseSettings

from settings.auth_settings import AuthSettings
from settings.bot_settings import BotSettings
from settings.db_settings import DatabaseSettings, RedisSettings
from settings.logging_settings import LoggingSettings
from settings.miscellaneous_settings import MiscellaneousSettings


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str('.env.prod'),
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    app_version: str = Field(default="1.0.0")
    is_development: bool = Field(default=False)
    environment: str = Field(default="test")
    token_blacklist: list[tuple[str | None, str | None]] = Field(default=[])

    postgres: DatabaseSettings = Field(default_factory=DatabaseSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    bot: BotSettings = Field(default_factory=BotSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
    misc: MiscellaneousSettings = Field(default_factory=MiscellaneousSettings)


@lru_cache(maxsize=1)
def get_app_settings() -> AppSettings:
    return AppSettings()
