from typing import Literal
from urllib.parse import quote

import boto3
from pydantic import PostgresDsn, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # JWT
    JWT_EXPIRATION: int = 15  # minutes
    JWT_KEY: str = "very-not-secure-jwt-key"
    JWT_ALG: str = "HS256"
    JWT_RF_TOKEN_EXPIRATION: int = 60 * 60 * 24 * 7  # 1 Week in  seconds

    # Database
    POSTGRES_USER: str = ""
    POSTGRES_PWD: str = ""
    POSTGRES_HOST: str = ""
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = ""

    REDIS_HOST: str = "127.0.0.1"
    REDIS_PORT: int = 6379

    ENVIRONMENT: Literal["development", "production"] = "development"

    CORS: list[str] = ["*"]

    # AWS Secrets
    AWS_REGION_NAME: str = ""

    @computed_field
    @property
    def DATABASE_URI(self) -> PostgresDsn:
        """
        Construct PostgreSQL connection URI.
        """
        if self.is_development:
            return PostgresDsn.build(
                scheme="postgresql+psycopg",
                username=self.POSTGRES_USER,
                password=self.POSTGRES_PWD,
                host=self.POSTGRES_HOST,
                port=self.POSTGRES_PORT,
                path=self.POSTGRES_DB,
            )
        auth_token = boto3.client(
            "rds", region_name=self.AWS_REGION_NAME
        ).generate_db_auth_token(
            DBHostname=self.POSTGRES_HOST,
            Port=self.POSTGRES_PORT,
            DBUsername=self.POSTGRES_USER,
            Region=self.AWS_REGION_NAME,
        )

        return PostgresDsn.build(
            scheme="postgresql+psycopg",
            username=self.POSTGRES_USER,
            password=quote(auth_token, safe=""),
            host=self.POSTGRES_HOST,
            port=self.POSTGRES_PORT,
            path=self.POSTGRES_DB,
        )

    @property
    def is_development(self):
        return self.ENVIRONMENT == "development"


settings = Settings()  # type: ignore
