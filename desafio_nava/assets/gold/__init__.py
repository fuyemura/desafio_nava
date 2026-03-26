"""
Camada Gold — Dados prontos para consumo analítico.

Os assets aqui consomem dados da camada Silver e produzem:
  - agregações de negócio
  - tabelas dimensionais e fatos
  - métricas e KPIs
  - datasets para BI / dashboards
"""

from desafio_nava.assets.gold.dim_faixa_etaria import dim_faixa_etaria
from desafio_nava.assets.gold.dim_operadora import dim_operadora
from desafio_nava.assets.gold.dim_municipio import dim_municipio
from desafio_nava.assets.gold.fato_beneficiario import fato_beneficiario

__all__ = ["dim_faixa_etaria", "dim_operadora", "dim_municipio", "fato_beneficiario"]
