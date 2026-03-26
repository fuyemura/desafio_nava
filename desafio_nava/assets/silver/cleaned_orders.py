from dagster import AssetExecutionContext, asset
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

from desafio_nava.assets.bronze.raw_orders import raw_orders
from desafio_nava.partitions import daily_partition


@asset(
    name="cleaned_orders",
    description="Pedidos com schema validado, duplicatas removidas e tipos corrigidos.",
    ins={"raw_orders": raw_orders},  # type: ignore[arg-type]
    partitions_def=daily_partition,
    metadata={"layer": "silver"},
    tags={"layer": "silver", "domain": "commerce"},
)
def cleaned_orders(context: AssetExecutionContext, raw_orders: DataFrame) -> DataFrame:
    """Limpa e valida o dataset de pedidos."""
    df = raw_orders

    before = df.count()

    # Remove duplicatas por order_id
    df = df.dropDuplicates(["order_id"])
    after_dedup = df.count()
    context.log.info(f"Duplicatas removidas: {before - after_dedup}")

    # Tipagem e conversões
    df = (
        df.withColumn("amount", F.col("amount").cast(DoubleType()))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )

    # Remove registros críticos nulos
    df = df.dropna(subset=["order_id", "customer_id", "amount"])

    final_count = df.count()
    context.add_output_metadata(
        {
            "num_rows": final_count,
            "duplicates_removed": before - after_dedup,
            "nulls_dropped": after_dedup - final_count,
            "schema": df.schema.simpleString(),
        }
    )
    return df
