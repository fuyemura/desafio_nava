"""
IO Managers customizados para persistência Delta Lake entre layers.
"""

import os

from dagster import ConfigurableIOManager, InputContext, OutputContext
from pyspark.sql import DataFrame


class DeltaLakeIOManager(ConfigurableIOManager):
    """
    IO Manager que persiste DataFrames PySpark como tabelas Delta Lake.

    Convenção de paths:
        {base_path}/{asset_group}/{asset_name}[/{partition_key}]
    """

    base_path: str = "./dagster_home/delta"
    write_mode: str = "overwrite"

    def _get_path(self, context: InputContext | OutputContext) -> str:
        parts = list(context.asset_key.path)
        return os.path.join(self.base_path, *parts).replace("\\", "/")

    def handle_output(self, context: OutputContext, obj: DataFrame) -> None:
        if obj is None:
            return
        path = self._get_path(context)
        mode = self.write_mode

        writer = obj.write.format("delta").mode(mode)

        if context.has_partition_key:
            writer = writer.option("replaceWhere", f"_partition='{context.partition_key}'")

        writer.save(path)
        context.log.info(f"Delta escrito em: {path}")
        context.add_output_metadata(
            {
                "delta_path": path,
                "num_rows": obj.count(),
                "schema": obj.schema.simpleString(),
            }
        )

    def load_input(self, context: InputContext) -> DataFrame:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError(
                "Nenhuma SparkSession ativa. Certifique-se de que o SparkResource "
                "foi inicializado antes de usar o DeltaLakeIOManager."
            )
        path = self._get_path(context)
        context.log.info(f"Lendo Delta de: {path}")
        return spark.read.format("delta").load(path)


delta_io_manager = DeltaLakeIOManager()

__all__ = ["DeltaLakeIOManager", "delta_io_manager"]

