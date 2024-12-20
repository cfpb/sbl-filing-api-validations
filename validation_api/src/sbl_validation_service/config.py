from enum import StrEnum
import os
from urllib import parse
from typing import Any

from pydantic import field_validator, ValidationInfo, BaseModel
from pydantic.networks import PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


env_files_to_load = [".env"]
if os.getenv("ENV", "LOCAL") == "LOCAL":
    env_files_to_load.append(".env.local")


class FsProtocol(StrEnum):
    FILE = "file"
    S3 = "s3"


class FsUploadConfig(BaseModel):
    protocol: str = FsProtocol.FILE.value
    root: str
    mkdir: bool = True


class FsDownloadConfig(BaseModel):
    protocol: str = FsProtocol.FILE.value
    target_protocol: str = None
    cache_storage: str = None
    check_files: bool = True
    version_aware: bool = True


class Settings(BaseSettings):
    db_schema: str = "public"
    db_name: str
    db_user: str
    db_pwd: str
    db_host: str
    db_scheme: str = "postgresql"
    conn: PostgresDsn | None = None

    fs_upload_config: FsUploadConfig
    fs_download_config: FsDownloadConfig

    expired_submission_check_secs: int = 120

    def __init__(self, **data):
        super().__init__(**data)

    @field_validator("conn", mode="before")
    @classmethod
    def build_postgres_dsn(cls, postgres_dsn, info: ValidationInfo) -> Any:
        postgres_dsn = PostgresDsn.build(
            scheme=info.data.get("db_scheme"),
            username=info.data.get("db_user"),
            password=parse.quote(info.data.get("db_pwd"), safe=""),
            host=info.data.get("db_host"),
            path=info.data.get("db_name"),
        )
        return str(postgres_dsn)

    model_config = SettingsConfigDict(env_file=env_files_to_load, extra="allow", env_nested_delimiter="__")


settings = Settings()
