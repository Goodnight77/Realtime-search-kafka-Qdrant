from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    qdrant_api_key: str
    qdrant_url: str

    collection_name: str = "stream_search"
    embed_model: str = "BAAI/bge-small-en-v1.5"
    sparse_embed_model: str = "Qdrant/bm25"
    embed_dim: int = 384

    window_size: int = 10000
    window_seconds: int = 86400

    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "stream_search"
    kafka_group_id: str = "stream_search_consumer"

    batch_size: int = 32
    batch_flush_ms: int = 500


settings = Settings()
