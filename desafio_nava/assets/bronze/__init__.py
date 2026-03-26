"""
Camada Bronze — Ingestão bruta de dados.

Os assets aqui representam dados coletados diretamente das fontes (APIs,
bancos de dados de origem, arquivos, etc.) sem nenhuma transformação.
O objetivo é preservar a fidelidade dos dados originais.
"""

from desafio_nava.assets.bronze.pda_beneficiarios import pda_beneficiarios

__all__ = ["pda_beneficiarios"]
