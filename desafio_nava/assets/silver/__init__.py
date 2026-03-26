"""
Camada Silver — Limpeza, validação e padronização.

Os assets aqui consomem dados da camada Bronze e aplicam:
  - remoção de duplicatas
  - validação de schema e tipos
  - padronização de campos (datas, strings, etc.)
  - enriquecimento básico
"""

from desafio_nava.assets.silver.cleaned_orders import cleaned_orders
from desafio_nava.assets.silver.cleaned_customers import cleaned_customers

__all__ = ["cleaned_orders", "cleaned_customers"]
