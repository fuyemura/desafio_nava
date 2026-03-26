from dagster import AssetExecutionContext, asset
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from desafio_nava.assets.bronze.raw_customers import raw_customers


@asset(
    name="cleaned_customers",
    description="Clientes com email validado, país padronizado e campos nulos tratados.",
    ins={"raw_customers": raw_customers},  # type: ignore[arg-type]
    metadata={"layer": "silver"},
    tags={"layer": "silver", "domain": "commerce"},
)
def cleaned_customers(
    context: AssetExecutionContext, raw_customers: DataFrame
) -> DataFrame:
    """Limpa e padroniza o dataset de clientes."""
    df = raw_customers

    df = df.dropDuplicates(["customer_id"])

    df = (
        df.withColumn("name", F.initcap(F.trim(F.col("name"))))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )

    df = df.dropna(subset=["customer_id", "email"])

    context.add_output_metadata({"num_rows": df.count(), "schema": df.schema.simpleString()})
    return df
