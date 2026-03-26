from dagster import asset, AssetKey, MaterializeResult, MetadataValue
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from desafio_nava.config.paths import BRONZE_PATHS, SILVER_PATHS

BRONZE_PATH = BRONZE_PATHS["raw_pda_beneficiario"]
SILVER_PATH = SILVER_PATHS["stg_pda_beneficiario"]


@asset(
    name="stg_pda_beneficiario",
    key_prefix=["silver"],
    compute_kind="spark",
    description=(
        "Transforma os beneficiários de planos de saúde (PDA/ANS) da camada Bronze "
        "para Silver: padroniza strings, faz cast de tipos numéricos, extrai ano/mês "
        "de competência e adiciona metadados de processamento."
    ),
    required_resource_keys={"spark"},
    deps=[AssetKey(["bronze", "raw_pda_beneficiario"])],
    tags={
        "layer": "silver",
        "domain": "saude",
        "source": "ans_pda",
    },
    metadata={
        "layer": "silver",
        "source_table": "bronze.raw_pda_beneficiario",
        "target_table": "silver.stg_pda_beneficiario",
        "partition_by": ["sg_uf", "id_cmpt_movel"],
    },
)
def stg_pda_beneficiario(context):
    spark = context.resources.spark

    context.log.info(f"Lendo Bronze Delta em: {BRONZE_PATH}")

    df_bronze = spark.read.format("delta").load(BRONZE_PATH)

    total_bronze = df_bronze.count()
    context.log.info(f"{total_bronze} registros lidos da Bronze")

    if total_bronze == 0:
        context.log.warning("Nenhum dado encontrado na tabela bronze.raw_pda_beneficiario")
        return MaterializeResult(
            metadata={
                "processamento": MetadataValue.json({
                    "registros_lidos": 0,
                    "registros_gravados": 0,
                    "status": "sem dados",
                })
            }
        )

    # -------------------------------------------------------------------------
    # Transformações Silver — schema alinhado ao DDL de silver.stg_pda_beneficiario
    # -------------------------------------------------------------------------
    df_silver = df_bronze.select(
        # Competência
        F.trim(F.col("ID_CMPT_MOVEL")).alias("ID_CMPT_MOVEL"),

        # Operadora
        F.trim(F.col("CD_OPERADORA")).alias("CD_OPERADORA"),
        F.trim(F.col("NM_RAZAO_SOCIAL")).alias("NM_RAZAO_SOCIAL"),
        F.trim(F.col("NR_CNPJ")).alias("NR_CNPJ"),
        F.trim(F.col("MODALIDADE_OPERADORA")).alias("MODALIDADE_OPERADORA"),

        # Localização
        F.trim(F.col("SG_UF")).alias("SG_UF"),
        F.trim(F.col("CD_MUNICIPIO")).alias("CD_MUNICIPIO"),
        F.trim(F.col("NM_MUNICIPIO")).alias("NM_MUNICIPIO"),

        # Beneficiário
        F.trim(F.col("TP_SEXO")).alias("TP_SEXO"),
        F.trim(F.col("DE_FAIXA_ETARIA")).alias("DE_FAIXA_ETARIA"),
        F.trim(F.col("DE_FAIXA_ETARIA_REAJ")).alias("DE_FAIXA_ETARIA_REAJ"),

        # Plano
        F.trim(F.col("CD_PLANO")).alias("CD_PLANO"),
        F.trim(F.col("TP_VIGENCIA_PLANO")).alias("TP_VIGENCIA_PLANO"),
        F.trim(F.col("DE_CONTRATACAO_PLANO")).alias("DE_CONTRATACAO_PLANO"),
        F.trim(F.col("DE_SEGMENTACAO_PLANO")).alias("DE_SEGMENTACAO_PLANO"),
        F.trim(F.col("DE_ABRG_GEOGRAFICA_PLANO")).alias("DE_ABRG_GEOGRAFICA_PLANO"),
        F.trim(F.col("COBERTURA_ASSIST_PLAN")).alias("COBERTURA_ASSIST_PLAN"),
        F.trim(F.col("TIPO_VINCULO")).alias("TIPO_VINCULO"),

        # Quantitativos — cast para INT (bronze armazena como STRING no metastore)
        F.col("QT_BENEFICIARIO_ATIVO").cast(IntegerType()).alias("QT_BENEFICIARIO_ATIVO"),
        F.col("QT_BENEFICIARIO_ADERIDO").cast(IntegerType()).alias("QT_BENEFICIARIO_ADERIDO"),
        F.col("QT_BENEFICIARIO_CANCELADO").cast(IntegerType()).alias("QT_BENEFICIARIO_CANCELADO"),

        # Metadados
        F.col("DT_CARGA").cast("date").alias("DT_CARGA"),
        F.current_timestamp().alias("CRIADO_EM"),
    )

    total_silver = df_silver.count()
    context.log.info(f"Gravando {total_silver} registros em {SILVER_PATH}")

    # Gravação na Silver, sobrescrevendo partições dinamicamente (idempotência)
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("SG_UF", "ID_CMPT_MOVEL")
        .option("overwriteSchema", "false")
        .save(f"{SILVER_PATH}")
    )

    context.log.info(f"Silver gravada com sucesso em: {SILVER_PATH}")

    return MaterializeResult(
        metadata={
            "processamento": MetadataValue.json({
                "registros_lidos": total_bronze,
                "registros_gravados": total_silver,
                "destino": SILVER_PATH,
                "status": "sucesso",
            })
        }
    )
