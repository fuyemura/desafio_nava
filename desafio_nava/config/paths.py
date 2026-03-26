BASE_DELTA_PATH = "D:/Projetos/desafio_nava/data"

SOURCE_PATHS = {
    "pda_beneficiario": f"{BASE_DELTA_PATH}/raw"
}

BRONZE_PATHS = {
    "raw_pda_beneficiario": f"{BASE_DELTA_PATH}/bronze/raw_pda_beneficiario",
}

SILVER_PATHS = {
    "stg_cotacao_historica": f"{BASE_DELTA_PATH}/silver/stg_cotacao_historica"
}

GOLD_PATHS = {
    "dim_ativo_financeiro": f"{BASE_DELTA_PATH}/gold/dim_ativo_financeiro",
    "fato_cotacao": f"{BASE_DELTA_PATH}/gold/fato_cotacao",
}