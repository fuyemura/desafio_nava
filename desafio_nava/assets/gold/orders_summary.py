from dagster import AssetExecutionContext, asset
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from desafio_nava.assets.silver.cleaned_customers import cleaned_customers
from desafio_nava.assets.silver.cleaned_orders import cleaned_orders


@asset(
    name="orders_summary",
    description="Resumo diário de pedidos por cliente com métricas de negócio.",
    ins={
        "cleaned_orders": cleaned_orders,  # type: ignore[arg-type]
        "cleaned_customers": cleaned_customers,  # type: ignore[arg-type]
    },
    metadata={"layer": "gold"},
    tags={"layer": "gold", "domain": "commerce"},
)
def orders_summary(
    context: AssetExecutionContext,
    cleaned_orders: DataFrame,
    cleaned_customers: DataFrame,
) -> DataFrame:
    """Agrega pedidos por cliente e calcula KPIs."""
    df = cleaned_orders.join(cleaned_customers, on="customer_id", how="left")

    summary = df.groupBy("customer_id", "name", "country").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("amount").alias("total_revenue"),
        F.avg("amount").alias("avg_order_value"),
        F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_orders"),
    )

    summary = summary.withColumn(
        "completion_rate", F.col("completed_orders") / F.col("total_orders")
    )

    total_revenue = summary.agg(F.sum("total_revenue")).collect()[0][0]
    context.add_output_metadata(
        {
            "num_customers": summary.count(),
            "total_revenue": float(total_revenue or 0),
            "schema": summary.schema.simpleString(),
        }
    )
    return summary
