import os
from pyspark.sql import SparkSession


# ===== PROJETOS DISPONÍVEIS =====
PROJECTS = {
    "financas": "D:/Projetos/DataLake",
    "desafio_nava": "D:/Projetos/desafio_nava/delta_lake",
}


def init_spark(app_name="MeuApp", project_name="financas"):
    """
    Inicializa Spark com Hive e Delta Lake persistente.

    Cada projeto possui warehouse e metastore completamente isolados
    (Cenário 1: projetos separados).

    Projetos disponíveis:
        - "financas"      → D:/Projetos/DataLake
        - "desafio_nava"  → D:/Projetos/desafio_nava/delta_lake

    Args:
        app_name (str):     Nome da aplicação exibido na Spark UI.
        project_name (str): Chave do projeto definida em PROJECTS.
    """

    # ===== VALIDAÇÃO DO PROJETO =====
    if project_name not in PROJECTS:
        raise ValueError(
            f"Projeto '{project_name}' não encontrado. "
            f"Opções disponíveis: {list(PROJECTS.keys())}"
        )

    # ===== CONFIGURAÇÃO DE DIRETÓRIOS =====
    BASE_DIR      = PROJECTS[project_name]
    WAREHOUSE_DIR = f"{BASE_DIR}/spark-warehouse"
    METASTORE_DIR = f"{BASE_DIR}/metastore_db"
    SCRATCH_DIR   = f"{BASE_DIR}/hive_scratch"

    # Criar diretórios necessários
    for dir_path in [WAREHOUSE_DIR, SCRATCH_DIR]:
        os.makedirs(dir_path, exist_ok=True)

    # ===== CONFIGURAÇÃO DO HADOOP (Windows) =====
    os.environ['HADOOP_HOME'] = r'D:\hadoop'

    # ===== SPARK SESSION =====
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Memória
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        # Shuffle / sort
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Hive Local (persistente e isolado por projeto)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.warehouse.dir", f"file:///{WAREHOUSE_DIR}")
        .config("hive.metastore.warehouse.dir", f"file:///{WAREHOUSE_DIR}")
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={METASTORE_DIR};create=true"
        )
        # Corrige problemas no Windows
        .config("hive.exec.scratchdir", SCRATCH_DIR)
        .config("hive.metastore.schema.verification", "false")
        .config("hive.metastore.schema.verification.record.version", "false")
        .config("datanucleus.schema.autoCreateAll", "true")
        .enableHiveSupport()
    )

    from delta import configure_spark_with_delta_pip
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"\n✅ Spark {spark.version} iniciado com Hive local persistente!")
    print(f"📦 Projeto:   {project_name}")
    print(f"📁 Warehouse: {WAREHOUSE_DIR}")
    print(f"📁 Metastore: {METASTORE_DIR}\n")

    return spark