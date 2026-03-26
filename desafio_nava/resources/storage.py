from dagster import ConfigurableResource
from pyspark.sql import DataFrame, SparkSession


class DeltaStorageResource(ConfigurableResource):
    """Resource para leitura e escrita de tabelas Delta Lake."""

    base_path: str = "./dagster_home/delta"

    def _path(self, layer: str, table: str) -> str:
        if layer not in ("bronze", "silver", "gold"):
            raise ValueError(f"Layer inválida: {layer}. Use 'bronze', 'silver' ou 'gold'.")
        return f"{self.base_path}/{layer}/{table}"

    def write(
        self,
        df: DataFrame,
        layer: str,
        table: str,
        mode: str = "append",
        partition_by: list[str] | None = None,
    ) -> str:
        path = self._path(layer, table)
        writer = df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return path

    def read(self, spark: SparkSession, layer: str, table: str) -> DataFrame:
        path = self._path(layer, table)
        return spark.read.format("delta").load(path)

    def merge(
        self,
        spark: SparkSession,
        new_df: DataFrame,
        layer: str,
        table: str,
        merge_condition: str,
    ) -> None:
        """Upsert incremental via DeltaTable merge."""
        from delta.tables import DeltaTable

        path = self._path(layer, table)
        target = DeltaTable.forPath(spark, path)
        (
            target.alias("tgt")
            .merge(new_df.alias("src"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
