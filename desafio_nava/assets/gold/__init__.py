"""
Camada Gold — Dados prontos para consumo analítico.

Os assets aqui consomem dados da camada Silver e produzem:
  - agregações de negócio
  - tabelas dimensionais e fatos
  - métricas e KPIs
  - datasets para BI / dashboards
"""

from desafio_nava.assets.gold.orders_summary import orders_summary

__all__ = ["orders_summary"]
