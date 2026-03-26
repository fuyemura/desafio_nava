from dagster import DailyPartitionsDefinition, MonthlyPartitionsDefinition

daily_partition = DailyPartitionsDefinition(start_date="2024-01-01")

monthly_partition = MonthlyPartitionsDefinition(start_date="2024-01-01")

__all__ = ["daily_partition", "monthly_partition"]
