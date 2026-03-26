from dagster import Definitions

# Import dos assets
from desafio_nava.assets.bronze.ing_pda_beneficiario import raw_pda_beneficiario
from desafio_nava.assets.silver.stg_pda_beneficiario import stg_pda_beneficiario
from desafio_nava.assets.gold.dim_operadora import dim_operadora
from desafio_nava.assets.gold.dim_municipio import dim_municipio

# Import do Spark resource
from desafio_nava.resources.spark_resource import resource_spark

# Definição do pipeline (Definitions)
defs = Definitions(
    assets=[
        raw_pda_beneficiario,
        stg_pda_beneficiario,
        dim_operadora,
        dim_municipio,
        ],
    resources={
        "spark": resource_spark
    }
)