from dagster import asset, AssetKey, MaterializeResult, MetadataValue
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from desafio_nava.config.paths import GOLD_PATHS, SILVER_PATHS

SILVER_PATH = SILVER_PATHS["stg_pda_beneficiario"]
GOLD_PATH = GOLD_PATHS["dim_municipio"]



@asset(
    name="dim_municipio",
    key_prefix=["gold"],
    compute_kind="spark",
    description=(
        "Dimensão SCD Tipo 1 de municípios. "
        "Insere novos registros e atualiza existentes quando algum atributo muda. "
        "SK gerada via xxhash64 determinístico sobre CD_MUNICIPIO."
    ),
    required_resource_keys={"spark"},
    deps=[AssetKey(["silver", "stg_pda_beneficiario"])],
    tags={
        "layer": "gold",
        "domain": "saude",
        "source": "ans_pda",
    },
    metadata={
        "layer": "gold",
        "source_table": "silver.stg_pda_beneficiario",
        "target_table": "gold.dim_municipio",
        "grain": "CD_MUNICIPIO",
        "scd_type": "1",
    },
)
def dim_municipio(context):
    spark = context.resources.spark

    context.log.info(f"Lendo Silver em: {SILVER_PATH}")

    now = F.current_timestamp()

    df_dim_municipio = (
        spark.read.format("delta").load(SILVER_PATH)
        .select("CD_MUNICIPIO", "NM_MUNICIPIO", "SG_UF")
        .dropDuplicates(["CD_MUNICIPIO"])
        .withColumn("SK_MUNICIPIO", F.abs(F.xxhash64(F.col("CD_MUNICIPIO"))))
        .withColumn("CRIADO_EM", now)
        .withColumn("ATUALIZADO_EM", now)
        .select(
            "SK_MUNICIPIO",
            "CD_MUNICIPIO",
            "NM_MUNICIPIO",
            "SG_UF",
            "CRIADO_EM",
            "ATUALIZADO_EM",
        )
    )

    total_municipio = df_dim_municipio.count()
    context.log.info(f"{total_municipio} municípios únicos encontrados na Silver")

    # Garante que a tabela Delta existe antes do MERGE (cria apenas se necessário)
    DeltaTable.createIfNotExists(spark).location(GOLD_PATH).addColumns(df_dim_municipio.schema).execute()

    context.log.info("Executando MERGE (SCD Tipo 1)")

    (
        DeltaTable.forPath(spark, GOLD_PATH).alias("target")
        .merge(
            df_dim_municipio.alias("source"),
            "target.CD_MUNICIPIO = source.CD_MUNICIPIO",
        )
        .whenMatchedUpdate(
            condition=(
                (F.col("source.NM_MUNICIPIO") != F.col("target.NM_MUNICIPIO")) |
                (F.col("source.SG_UF") != F.col("target.SG_UF"))
            ),
            set={
                "NM_MUNICIPIO": "source.NM_MUNICIPIO",
                "SG_UF": "source.SG_UF",
                "ATUALIZADO_EM": "current_timestamp()",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    op_metrics = (
        DeltaTable.forPath(spark, GOLD_PATH)
        .history(1)
        .collect()[0]["operationMetrics"]
    )
    inserted = int(op_metrics.get("numTargetRowsInserted", 0))
    updated = int(op_metrics.get("numTargetRowsUpdated", 0))

    context.log.info(f"Concluído — inseridos: {inserted}, atualizados: {updated}")

    return MaterializeResult(
        metadata={
            "processamento": MetadataValue.json({
                "registros_total": total_municipio,
                "registros_inseridos": inserted,
                "registros_atualizados": updated,
                "destino": GOLD_PATH,
                "status": "sucesso",
            })
        }
    )
