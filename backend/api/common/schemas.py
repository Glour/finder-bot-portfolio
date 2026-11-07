import datetime
from uuid import UUID

from pydantic import BaseModel


class JwtUserSchema(BaseModel):
    id: str
    role: str
    permissions: dict[str, list[str]]


class JwtClaims(BaseModel):
    user: JwtUserSchema
    access_jti: str
    refresh_jti: str
    type: str
    exp: int
    iat: int


class TimestampSchemaMixin(BaseModel):
    created_at: datetime.datetime
    modified_at: datetime.datetime


class UUIDSchemaMixin(BaseModel):
    id: UUID
