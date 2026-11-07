from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BotSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore"
    )

    test_token: str = Field(default="")
    dev_token: str = Field(default="")
    prod_token: str = Field(default="")
    en_prod_token: str = Field(default="")
    tech_prod_token: str = Field(default="")

    admin_ids: list[int] = Field(default=[])
    use_redis: bool = Field(default=True)

    test_channel_id: int | None = Field(default=None)
    dev_channel_id: int | None = Field(default=None)
    prod_channel_id: int | None = Field(default=None)
    en_prod_channel_id: int | None = Field(default=None)
    tech_prod_channel_id: int | None = Field(default=None)
    alerts_channel_id: int | None = Field(default=None)
    alerts_topic_id: int | None = Field(default=None)

    test_topic_very_high_id: str
    test_topic_high_id: str
    test_topic_medium_id: str
    test_topic_low_id: str

    dev_topic_very_high_id: str
    dev_topic_high_id: str
    dev_topic_medium_id: str
    dev_topic_low_id: str
    dev_topic_delay_id: str

    prod_topic_very_high_id: str
    prod_topic_high_id: str
    prod_topic_medium_id: str
    prod_topic_low_id: str

    en_prod_topic_very_high_id: str
    en_prod_topic_high_id: str
    en_prod_topic_medium_id: str
    en_prod_topic_low_id: str

    tech_prod_topic_very_high_id: str
    tech_prod_topic_high_id: str
    tech_prod_topic_medium_id: str
    tech_prod_topic_low_id: str
