from dagster import asset, AssetKey, MaterializeResult, MetadataValue
from pyspark.sql import functions as F

from desafio_nava.config.paths import GOLD_PATHS, SILVER_PATHS

SILVER_PATH = SILVER_PATHS["stg_pda_beneficiario"]
GOLD_PATH = GOLD_PATHS["fato_beneficiario"]

DIM_OPERADORA_PATH = GOLD_PATHS["dim_operadora"]
DIM_MUNICIPIO_PATH = GOLD_PATHS["dim_municipio"]
DIM_FAIXA_ETARIA_PATH = GOLD_PATHS["dim_faixa_etaria"]


@asset(
    name="fato_beneficiario",
    key_prefix=["gold"],
    compute_kind="spark",
    description=(
        "Tabela fato de beneficiários de planos de saúde. "
        "Resolve surrogate keys a partir das dimensões e agrega métricas de beneficiários. "
        "Idempotente por SG_UF + ID_CMPT_MOVEL via partitionOverwriteMode=dynamic."
    ),
    required_resource_keys={"spark"},
    deps=[
        AssetKey(["silver", "stg_pda_beneficiario"]),
        AssetKey(["gold", "dim_operadora"]),
        AssetKey(["gold", "dim_municipio"]),
        AssetKey(["gold", "dim_faixa_etaria"]),
    ],
    tags={
        "layer": "gold",
        "domain": "saude",
        "source": "ans_pda",
    },
    metadata={
        "layer": "gold",
        "source_table": "silver.stg_pda_beneficiario",
        "target_table": "gold.fato_beneficiario",
        "partition_by": ["SG_UF", "ID_CMPT_MOVEL"],
        "idempotency": "dynamic partition overwrite by SG_UF + ID_CMPT_MOVEL",
    },
)
def fato_beneficiario(context):
    spark = context.resources.spark

    context.log.info(f"Lendo Silver em: {SILVER_PATH}")
    df_silver = spark.read.format("delta").load(SILVER_PATH)

    total_silver = df_silver.count()
    context.log.info(f"{total_silver} registros lidos da Silver")

    if total_silver == 0:
        context.log.warning("Nenhum dado encontrado na tabela silver.stg_pda_beneficiario")
        return MaterializeResult(
            metadata={
                "processamento": MetadataValue.json({
                    "registros_lidos": 0,
                    "registros_gravados": 0,
                    "status": "sem dados",
                })
            }
        )

    # Carrega dimensões para resolução de surrogate keys
    df_dim_operadora = (
        spark.read.format("delta").load(DIM_OPERADORA_PATH)
        .select("CD_OPERADORA", "SK_OPERADORA")
    )
    df_dim_municipio = (
        spark.read.format("delta").load(DIM_MUNICIPIO_PATH)
        .select("CD_MUNICIPIO", "SK_MUNICIPIO")
    )
    df_dim_faixa_etaria = (
        spark.read.format("delta").load(DIM_FAIXA_ETARIA_PATH)
        .select("DE_FAIXA_ETARIA", "SK_FAIXA_ETARIA")
    )

    # Resolve surrogate keys e projeta colunas da fato
    df_fato = (
        df_silver
        .join(df_dim_operadora, on="CD_OPERADORA", how="left")
        .join(df_dim_municipio, on="CD_MUNICIPIO", how="left")
        .join(df_dim_faixa_etaria, on="DE_FAIXA_ETARIA", how="left")
        .select(
            F.col("SG_UF"),
            F.col("ID_CMPT_MOVEL"),
            F.col("SK_OPERADORA"),
            F.col("SK_MUNICIPIO"),
            F.col("SK_FAIXA_ETARIA"),
            F.col("QT_BENEFICIARIO_ATIVO"),
            F.col("QT_BENEFICIARIO_ADERIDO"),
            F.col("QT_BENEFICIARIO_CANCELADO"),
            F.current_timestamp().alias("CRIADO_EM"),
        )
    )

    total_fato = df_fato.count()
    context.log.info(f"Gravando {total_fato} registros em {GOLD_PATH}")

    # Idempotência: sobrescreve apenas as partições presentes no DataFrame (SG_UF + ID_CMPT_MOVEL)
    (
        df_fato.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("SG_UF", "ID_CMPT_MOVEL")
        .option("overwriteSchema", "false")
        .save(GOLD_PATH)
    )

    context.log.info(f"Fato gravada com sucesso em: {GOLD_PATH}")

    return MaterializeResult(
        metadata={
            "processamento": MetadataValue.json({
                "registros_lidos": total_silver,
                "registros_gravados": total_fato,
                "destino": GOLD_PATH,
                "status": "sucesso",
            })
        }
    )
