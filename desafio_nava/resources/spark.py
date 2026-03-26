import os

from dagster import ConfigurableResource
from pyspark.sql import SparkSession


class SparkResource(ConfigurableResource):
    """Resource que fornece uma SparkSession configurada com Delta Lake."""

    app_name: str = "desafio_nava"
    master: str = "local[*]"
    delta_warehouse_path: str = "./dagster_home/delta"
    extra_configs: dict[str, str] = {}

    def get_session(self) -> SparkSession:
        builder = (
            SparkSession.builder.master(self.master)
            .appName(self.app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", self.delta_warehouse_path)
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        )

        for key, value in self.extra_configs.items():
            builder = builder.config(key, value)

        from delta import configure_spark_with_delta_pip

        return configure_spark_with_delta_pip(builder).getOrCreate()
