from dagster import Definitions

# Import dos assets
from desafio_nava.assets.bronze.ing_pda_beneficiario import raw_pda_beneficiario
from desafio_nava.assets.silver.stg_pda_beneficiario import stg_pda_beneficiario

# Import do Spark resource
from desafio_nava.resources.spark_resource import resource_spark

# Definição do pipeline (Definitions)
defs = Definitions(
    assets=[
        raw_pda_beneficiario,
        stg_pda_beneficiario,
        ],
    resources={
        "spark": resource_spark
    }
)