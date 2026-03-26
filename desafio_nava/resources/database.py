from dagster import ConfigurableResource
from sqlalchemy import Engine, create_engine, text


class PostgresResource(ConfigurableResource):
    """Resource para conexão com PostgreSQL via SQLAlchemy."""

    host: str
    port: int = 5432
    database: str
    username: str
    password: str

    def get_engine(self) -> Engine:
        url = (
            f"postgresql+psycopg2://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        return create_engine(url, pool_pre_ping=True)

    def execute(self, query: str, params: dict | None = None):
        with self.get_engine().connect() as conn:
            return conn.execute(text(query), params or {})
