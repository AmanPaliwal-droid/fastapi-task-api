from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    mongodb_url: str = "mongodb://root:rootpassword@localhost:27017/taskdb?authSource=admin"
    mongodb_db_name: str = "taskdb"
    app_env: str = "development"

    # Task processing
    max_retries: int = 3
    failure_simulation_rate: float = 0.30  # 30% artificial failure
    task_lock_ttl: int = 300  # seconds — lock expires if worker dies


settings = Settings()
