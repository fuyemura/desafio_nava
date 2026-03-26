from dagster import AssetSelection, define_asset_job

# Job que materializa toda a pipeline de ponta a ponta
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    description="Materializa todos os assets do Bronze ao Gold.",
    selection=AssetSelection.all(),
)

# Job que materializa apenas a ingestão (Bronze)
ingestion_job = define_asset_job(
    name="ingestion_job",
    description="Ingere dados brutos para a camada Bronze.",
    selection=AssetSelection.groups("bronze"),
)

# Job que executa somente as transformações Silver → Gold
transform_job = define_asset_job(
    name="transform_job",
    description="Transforma e agrega dados das camadas Silver e Gold.",
    selection=AssetSelection.groups("silver") | AssetSelection.groups("gold"),
)

all_jobs = [full_pipeline_job, ingestion_job, transform_job]
