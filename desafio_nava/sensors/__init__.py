from dagster import (
    AssetSelection,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    asset_sensor,
    sensor,
)

from desafio_nava.assets.bronze.raw_orders import raw_orders
from desafio_nava.jobs import transform_job


@asset_sensor(
    asset_key=raw_orders.key,
    job=transform_job,
    name="new_orders_sensor",
    description="Dispara o job de transformação sempre que novos pedidos brutos são materializados.",
    minimum_interval_seconds=60,
)
def new_orders_sensor(context: SensorEvaluationContext, asset_event):
    """Sensor reativo: ao detectar novos raw_orders, inicia o transform_job."""
    yield RunRequest(
        run_key=context.cursor,
        tags={"triggered_by": "new_orders_sensor"},
    )


all_sensors = [new_orders_sensor]
