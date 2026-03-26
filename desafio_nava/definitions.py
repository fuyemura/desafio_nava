from dagster import Definitions, load_assets_from_package_module

from desafio_nava.assets import bronze, gold, silver
from desafio_nava.jobs import all_jobs
from desafio_nava.partitions import daily_partition, monthly_partition
from desafio_nava.resources import get_resources_for_env
from desafio_nava.schedules import all_schedules
from desafio_nava.sensors import all_sensors

all_assets = [
    *load_assets_from_package_module(bronze, group_name="bronze"),
    *load_assets_from_package_module(silver, group_name="silver"),
    *load_assets_from_package_module(gold, group_name="gold"),
]

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
    resources=get_resources_for_env(),
)
