from dagster import resource
from desafio_nava.utils.spark_config import init_spark

@resource
def resource_spark(_):
    spark = init_spark(
        app_name="DesafioNavaApp",
        project_name="desafio_nava"
    )
    try:
        yield spark
    finally:
        try:
            spark.stop()
        except Exception:
            pass