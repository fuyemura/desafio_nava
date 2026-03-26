from dagster import ScheduleDefinition

from desafio_nava.jobs import full_pipeline_job, ingestion_job

# Pipeline completa executada diariamente às 02:00 (horário de Brasília, UTC-3)
daily_full_pipeline_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 5 * * *",  # 05:00 UTC = 02:00 BRT
    name="daily_full_pipeline_schedule",
    description="Executa a pipeline completa Bronze → Silver → Gold todo dia às 02:00 BRT.",
)

# Ingestão a cada hora durante o horário comercial (07:00–22:00 UTC)
hourly_ingestion_schedule = ScheduleDefinition(
    job=ingestion_job,
    cron_schedule="0 7-22 * * *",
    name="hourly_ingestion_schedule",
    description="Ingere dados brutos a cada hora durante o horário comercial.",
)

all_schedules = [daily_full_pipeline_schedule, hourly_ingestion_schedule]
