from pydantic import Field, RedisDsn
from pydantic_settings import BaseSettings

from sqlalchemy.engine.url import URL


class DatabaseSettings(BaseSettings):
    db_host: str
    db_password: str
    db_user: str
    db_name: str
    db_port: int = Field(default=5432)

    # For SQLAlchemy
    @property
    def construct_sqlalchemy_url(self, driver="asyncpg", host=None, port=None) -> str:
        if not host:
            host = self.db_host
        if not port:
            port = self.db_port
        uri = URL.create(
            drivername=f"postgresql+{driver}",
            username=self.db_user,
            password=self.db_password,
            host=host,
            port=port,
            database=self.db_name,
        )
        return uri.render_as_string(hide_password=False)


class RedisSettings(BaseSettings):
    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)
    redis_db: int = Field(default=0)
    redis_password: str | None = Field(default=None)

    @property
    def dsn(self) -> str:
        return str(RedisDsn.build(scheme="redis", host=self.redis_host, port=self.redis_port))
