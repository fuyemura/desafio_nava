from dagster import asset, AssetKey, MaterializeResult, MetadataValue
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from desafio_nava.config.paths import GOLD_PATHS, SILVER_PATHS

SILVER_PATH = SILVER_PATHS["stg_pda_beneficiario"]
GOLD_PATH = GOLD_PATHS["dim_operadora"]


@asset(
    name="dim_operadora",
    key_prefix=["gold"],
    compute_kind="spark",
    description=(
        "Dimensão SCD Tipo 1 de operadoras de planos de saúde. "
        "Insere novos registros e atualiza existentes quando algum atributo muda. "
        "SK gerada via xxhash64 determinístico sobre CD_OPERADORA."
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
        "target_table": "gold.dim_operadora",
        "grain": "CD_OPERADORA",
        "scd_type": "1",
    },
)
def dim_operadora(context):
    spark = context.resources.spark

    context.log.info(f"Lendo Silver em: {SILVER_PATH}")

    now = F.current_timestamp()

    df_dim_operadora = (
        spark.read.format("delta").load(SILVER_PATH)
        .select("CD_OPERADORA", "NM_RAZAO_SOCIAL", "NR_CNPJ", "MODALIDADE_OPERADORA")
        .dropDuplicates(["CD_OPERADORA"])
        .withColumn("SK_OPERADORA", F.abs(F.xxhash64(F.col("CD_OPERADORA"))))
        .withColumn("CRIADO_EM", now)
        .withColumn("ATUALIZADO_EM", now)
        .select(
            "SK_OPERADORA",
            "CD_OPERADORA",
            "NM_RAZAO_SOCIAL",
            "NR_CNPJ",
            "MODALIDADE_OPERADORA",
            "CRIADO_EM",
            "ATUALIZADO_EM",
        )
    )

    total_operadora = df_dim_operadora.count()
    context.log.info(f"{total_operadora} operadoras únicas encontradas na Silver")

    # Garante que a tabela Delta existe antes do MERGE (cria apenas se necessário)
    DeltaTable.createIfNotExists(spark).location(GOLD_PATH).addColumns(df_dim_operadora.schema).execute()

    context.log.info("Executando MERGE (SCD Tipo 1)")

    (
        DeltaTable.forPath(spark, GOLD_PATH).alias("target")
        .merge(
            df_dim_operadora.alias("source"),
            "target.CD_OPERADORA = source.CD_OPERADORA",
        )
        .whenMatchedUpdate(
            condition=(
                (F.col("source.NM_RAZAO_SOCIAL") != F.col("target.NM_RAZAO_SOCIAL")) |
                (F.col("source.NR_CNPJ") != F.col("target.NR_CNPJ")) |
                (F.col("source.MODALIDADE_OPERADORA") != F.col("target.MODALIDADE_OPERADORA"))
            ),
            set={
                "NM_RAZAO_SOCIAL": "source.NM_RAZAO_SOCIAL",
                "NR_CNPJ": "source.NR_CNPJ",
                "MODALIDADE_OPERADORA": "source.MODALIDADE_OPERADORA",
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
                "registros_total": total_operadora,
                "registros_inseridos": inserted,
                "registros_atualizados": updated,
                "destino": GOLD_PATH,
                "status": "sucesso",
            })
        }
      )
