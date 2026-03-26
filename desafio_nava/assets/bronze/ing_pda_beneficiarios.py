import glob

from dagster import AssetExecutionContext, asset
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from desafio_nava.config.paths import SOURCE_PATHS, BRONZE_PATHS
from desafio_nava.resources.spark import SparkResource

SOURCE_PATH_PDA_BENEFICIARIO = f"{SOURCE_PATHS['pda_beneficiario']}/pda-024-icb-*.csv"
BRONZE_PATH = BRONZE_PATHS["raw_pda_beneficiario"]

PDA_BENEFICIARIO_SCHEMA = StructType(
    [
        StructField("ID_CMPT_MOVEL", StringType(), nullable=False),
        StructField("CD_OPERADORA", StringType(), nullable=False),
        StructField("NM_RAZAO_SOCIAL", StringType(), nullable=True),
        StructField("NR_CNPJ", StringType(), nullable=True),
        StructField("MODALIDADE_OPERADORA", StringType(), nullable=True),
        StructField("SG_UF", StringType(), nullable=False),
        StructField("CD_MUNICIPIO", StringType(), nullable=True),
        StructField("NM_MUNICIPIO", StringType(), nullable=True),
        StructField("TP_SEXO", StringType(), nullable=True),
        StructField("DE_FAIXA_ETARIA", StringType(), nullable=True),
        StructField("DE_FAIXA_ETARIA_REAJ", StringType(), nullable=True),
        StructField("CD_PLANO", StringType(), nullable=True),
        StructField("TP_VIGENCIA_PLANO", StringType(), nullable=True),
        StructField("DE_CONTRATACAO_PLANO", StringType(), nullable=True),
        StructField("DE_SEGMENTACAO_PLANO", StringType(), nullable=True),
        StructField("DE_ABRG_GEOGRAFICA_PLANO", StringType(), nullable=True),
        StructField("COBERTURA_ASSIST_PLAN", StringType(), nullable=True),
        StructField("TIPO_VINCULO", StringType(), nullable=True),
        StructField("QT_BENEFICIARIO_ATIVO", IntegerType(), nullable=True),
        StructField("QT_BENEFICIARIO_ADERIDO", IntegerType(), nullable=True),
        StructField("QT_BENEFICIARIO_CANCELADO", IntegerType(), nullable=True),
        StructField("DT_CARGA", DateType(), nullable=True),
    ]
)

@asset(
    name="raw_pda_beneficiario",
    key_prefix=["bronze"],
    compute_kind="spark",
    required_resource_keys={"spark"},
    description=(
        "Ingestão bruta dos beneficiários de planos de saúde (PDA/ANS). "
        "Fonte: arquivo CSV semicolonado gerado pela ANS, carregado sem transformações."
    ),
    metadata={
        "layer": "bronze",
        "source": SOURCE_PATH_PDA_BENEFICIARIO,
        "partition_by": ["SG_UF", "ID_CMPT_MOVEL"],
    },
    tags={"layer": "bronze", "domain": "saude", "source": "ans_pda"},
)
def raw_pda_beneficiarios(context):
    """Cria a Delta Table via Spark SQL (se não existir) e insere os dados dos CSVs encontrados."""
    spark = context.resources.spark

    # Descobre os arquivos CSV que correspondem ao padrão
    csv_files = glob.glob(SOURCE_PATH_PDA_BENEFICIARIO)

    if not csv_files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado com o padrão: {SOURCE_PATH_PDA_BENEFICIARIO}"
        )

    context.log.info(f"{len(csv_files)} arquivo(s) encontrado(s): {csv_files}")

    # Leitura de todos os CSVs com schema explícito
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("quote", '"')
        .option("encoding", "UTF-8")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(PDA_BENEFICIARIO_SCHEMA)
        .load(csv_files)
    )

    # Adiciona timestamp de ingestão
    df = df.withColumn("CRIADO_EM", F.current_timestamp())

    # Sobrescreve apenas as partições presentes no DataFrame (idempotência por SG_UF + ID_CMPT_MOVEL)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("SG_UF", "ID_CMPT_MOVEL")
        .option("overwriteSchema", "false")
        .saveAsTable(BRONZE_PATH)
    )
    context.log.info(f"Dados carregados na tabela '{BRONZE_PATH}'.")

    context.add_output_metadata(
        {
            "num_rows": df.count(),
            "schema": df.schema.simpleString(),
            "table": BRONZE_PATH,
            "partitions": ["SG_UF", "ID_CMPT_MOVEL"],
            "files_loaded": csv_files,
        }
    )
