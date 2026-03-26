from dagster import asset, AssetKey, MaterializeResult, MetadataValue
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from desafio_nava.config.paths import SILVER_PATHS

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

    context.log.info("Lendo bronze.raw_pda_beneficiario")

    df_bronze = spark.sql("SELECT * FROM bronze.raw_pda_beneficiario")

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
        F.trim(F.col("ID_CMPT_MOVEL")).alias("id_cmpt_movel"),

        # Operadora
        F.trim(F.col("CD_OPERADORA")).alias("cd_operadora"),
        F.trim(F.col("NM_RAZAO_SOCIAL")).alias("nm_razao_social"),
        F.trim(F.col("NR_CNPJ")).alias("nr_cnpj"),
        F.trim(F.col("MODALIDADE_OPERADORA")).alias("modalidade_operadora"),

        # Localização
        F.trim(F.col("SG_UF")).alias("sg_uf"),
        F.trim(F.col("CD_MUNICIPIO")).alias("cd_municipio"),
        F.trim(F.col("NM_MUNICIPIO")).alias("nm_municipio"),

        # Beneficiário
        F.trim(F.col("TP_SEXO")).alias("tp_sexo"),
        F.trim(F.col("DE_FAIXA_ETARIA")).alias("de_faixa_etaria"),
        F.trim(F.col("DE_FAIXA_ETARIA_REAJ")).alias("de_faixa_etaria_reaj"),

        # Plano
        F.trim(F.col("CD_PLANO")).alias("cd_plano"),
        F.trim(F.col("TP_VIGENCIA_PLANO")).alias("tp_vigencia_plano"),
        F.trim(F.col("DE_CONTRATACAO_PLANO")).alias("de_contratacao_plano"),
        F.trim(F.col("DE_SEGMENTACAO_PLANO")).alias("de_segmentacao_plano"),
        F.trim(F.col("DE_ABRG_GEOGRAFICA_PLANO")).alias("de_abrg_geografica_plano"),
        F.trim(F.col("COBERTURA_ASSIST_PLAN")).alias("cobertura_assist_plan"),
        F.trim(F.col("TIPO_VINCULO")).alias("tipo_vinculo"),

        # Quantitativos — cast para INT (bronze armazena como STRING no metastore)
        F.col("QT_BENEFICIARIO_ATIVO").cast(IntegerType()).alias("qt_beneficiario_ativo"),
        F.col("QT_BENEFICIARIO_ADERIDO").cast(IntegerType()).alias("qt_beneficiario_aderido"),
        F.col("QT_BENEFICIARIO_CANCELADO").cast(IntegerType()).alias("qt_beneficiario_cancelado"),

        # Metadados
        F.col("DT_CARGA").cast("date").alias("dt_carga"),
        F.current_timestamp().alias("criado_em"),
    )

    total_silver = df_silver.count()
    context.log.info(f"Gravando {total_silver} registros em {SILVER_PATH}")

    # Gravação na Silver, sobrescrevendo partições dinamicamente (idempotência)
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("sg_uf", "id_cmpt_movel")
        .option("overwriteSchema", "true")
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
