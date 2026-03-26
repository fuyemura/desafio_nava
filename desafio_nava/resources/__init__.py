import os

from desafio_nava.resources.database import PostgresResource
from desafio_nava.resources.spark import SparkResource
from desafio_nava.resources.storage import DeltaStorageResource

# Resources compartilhados entre ambientes
_base_resources = {}

# Resources por ambiente
_dev_resources = {
    **_base_resources,
    "database": PostgresResource(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "desafio_nava"),
        username=os.getenv("POSTGRES_USER", "dagster"),
        password=os.getenv("POSTGRES_PASSWORD", "dagster"),
    ),
    "spark": SparkResource(
        app_name="desafio_nava_dev",
        master=os.getenv("SPARK_MASTER", "local[*]"),
        delta_warehouse_path=os.getenv("DELTA_WAREHOUSE_PATH", "./dagster_home/delta"),
    ),
    "storage": DeltaStorageResource(
        base_path=os.getenv("DELTA_WAREHOUSE_PATH", "./dagster_home/delta"),
    ),
}

_prod_resources = {
    **_base_resources,
    "database": PostgresResource(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        database=os.environ["POSTGRES_DB"],
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ),
    "spark": SparkResource(
        app_name="desafio_nava_prod",
        master=os.environ.get("SPARK_MASTER", "local[*]"),
        delta_warehouse_path=os.environ["DELTA_WAREHOUSE_PATH"],
    ),
    "storage": DeltaStorageResource(
        base_path=os.environ["DELTA_WAREHOUSE_PATH"],
    ),
}


def get_resources_for_env() -> dict:
    env = os.getenv("ENV", "development")
    if env == "production":
        return _prod_resources
    return _dev_resources
