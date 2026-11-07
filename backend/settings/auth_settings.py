from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AuthSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore"
    )

    mexc_access_key: str = Field(default="")
    mexc_secret_key: str = Field(default="")
