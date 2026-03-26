from dagster import asset, AssetKey, MaterializeResult, MetadataValue
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from desafio_nava.config.paths import GOLD_PATHS, SILVER_PATHS

SILVER_PATH = SILVER_PATHS["stg_pda_beneficiario"]
GOLD_PATH = GOLD_PATHS["dim_faixa_etaria"]


@asset(
    name="dim_faixa_etaria",
    key_prefix=["gold"],
    compute_kind="spark",
    description=(
        "Dimensão SCD Tipo 1 de faixas etárias. "
        "Insere novos registros e atualiza existentes quando algum atributo muda. "
        "SK gerada via xxhash64 determinístico sobre DE_FAIXA_ETARIA."
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
        "target_table": "gold.dim_faixa_etaria",
        "grain": "DE_FAIXA_ETARIA",
        "scd_type": "1",
    },
)
def dim_faixa_etaria(context):
    spark = context.resources.spark

    context.log.info(f"Lendo Silver em: {SILVER_PATH}")

    now = F.current_timestamp()

    df_dim_faixa_etaria = (
        spark.read.format("delta").load(SILVER_PATH)
        .select("DE_FAIXA_ETARIA", "DE_FAIXA_ETARIA_REAJ")
        .dropDuplicates(["DE_FAIXA_ETARIA"])
        .withColumn("SK_FAIXA_ETARIA", F.abs(F.xxhash64(F.col("DE_FAIXA_ETARIA"))))
        .withColumn("CRIADO_EM", now)
        .withColumn("ATUALIZADO_EM", now)
        .select(
            "SK_FAIXA_ETARIA",
            "DE_FAIXA_ETARIA",
            "DE_FAIXA_ETARIA_REAJ",
            "CRIADO_EM",
            "ATUALIZADO_EM",
        )
    )

    total_faixa_etaria = df_dim_faixa_etaria.count()
    context.log.info(f"{total_faixa_etaria} faixas etárias únicas encontradas na Silver")

    # Garante que a tabela Delta existe antes do MERGE (cria apenas se necessário)
    DeltaTable.createIfNotExists(spark).location(GOLD_PATH).addColumns(df_dim_faixa_etaria.schema).execute()

    context.log.info("Executando MERGE (SCD Tipo 1)")

    (
        DeltaTable.forPath(spark, GOLD_PATH).alias("target")
        .merge(
            df_dim_faixa_etaria.alias("source"),
            "target.DE_FAIXA_ETARIA = source.DE_FAIXA_ETARIA",
        )
        .whenMatchedUpdate(
            condition=(
                F.col("source.DE_FAIXA_ETARIA_REAJ") != F.col("target.DE_FAIXA_ETARIA_REAJ")
            ),
            set={
                "DE_FAIXA_ETARIA_REAJ": "source.DE_FAIXA_ETARIA_REAJ",
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
                "registros_total": total_faixa_etaria,
                "registros_inseridos": inserted,
                "registros_atualizados": updated,
                "destino": GOLD_PATH,
                "status": "sucesso",
            })
        }
    )
