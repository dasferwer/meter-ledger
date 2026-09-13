from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://billing:billing@database:5432/billing"
    rabbitmq_url: str = "amqp://billing:billing@rabbitmq:5672/"
    admin_token: str = Field(
        default="local-demo-meterledger-admin-change-before-deployment", min_length=32
    )
    testing: bool = False
    worker_before_commit_delay: float = Field(default=0, ge=0, le=30)
    dispatcher_after_publish_delay: float = Field(default=0, ge=0, le=30)


settings = Settings()
