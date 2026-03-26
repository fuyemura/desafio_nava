"""
Script para baixar todos os arquivos de beneficiários (ANS) - competência 202508
Fonte: https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios-024/202508/
"""

import os
import urllib.request
from pathlib import Path

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios-024/202508/"

ESTADOS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO", "XX"
]

DESTINO = Path("ans_beneficiarios_202508")


def baixar_arquivo(url: str, destino: Path) -> None:
    nome = destino.name
    print(f"  Baixando {nome}...", end=" ", flush=True)
    try:
        urllib.request.urlretrieve(url, destino)
        tamanho_mb = destino.stat().st_size / (1024 * 1024)
        print(f"OK ({tamanho_mb:.1f} MB)")
    except Exception as e:
        print(f"ERRO: {e}")


def main():
    DESTINO.mkdir(parents=True, exist_ok=True)
    print(f"Diretório de destino: {DESTINO.resolve()}\n")

    for uf in ESTADOS:
        filename = f"pda-024-icb-{uf}-2025_08.zip"
        url = BASE_URL + filename
        caminho = DESTINO / filename

        if caminho.exists():
            print(f"  {filename} já existe, pulando.")
            continue

        baixar_arquivo(url, caminho)

    print("\nDownload concluído!")
    arquivos = list(DESTINO.glob("*.zip"))
    total_mb = sum(f.stat().st_size for f in arquivos) / (1024 * 1024)
    print(f"Total: {len(arquivos)} arquivos | {total_mb:.1f} MB")


if __name__ == "__main__":
    main()
