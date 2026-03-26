"""
Camada Bronze — Ingestão bruta de dados.

Os assets aqui representam dados coletados diretamente das fontes (APIs,
bancos de dados de origem, arquivos, etc.) sem nenhuma transformação.
O objetivo é preservar a fidelidade dos dados originais.
"""

from desafio_nava.assets.bronze.ing_pda_beneficiario import raw_pda_beneficiario

__all__ = ["raw_pda_beneficiario"]
