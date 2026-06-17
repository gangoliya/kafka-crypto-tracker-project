from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # These will be overwritten if they exist in the .env file
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_NAME: str = "market_events"
    
    model_config = SettingsConfigDict(env_file=".env",  env_file_encoding="utf-8", extra="ignore")

settings = Settings()
