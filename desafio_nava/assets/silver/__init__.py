"""
Camada Silver — Limpeza, validação e padronização.

Os assets aqui consomem dados da camada Bronze e aplicam:
  - remoção de duplicatas
  - validação de schema e tipos
  - padronização de campos (datas, strings, etc.)
  - enriquecimento básico
"""

from desafio_nava.assets.silver.stg_pda_beneficiario import stg_pda_beneficiario

__all__ = ["stg_pda_beneficiario"]
