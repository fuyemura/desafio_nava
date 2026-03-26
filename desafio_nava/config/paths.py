BASE_DELTA_PATH = "D:/Projetos/desafio_nava/data"

SOURCE_PATHS = {
    "pda_beneficiario": f"{BASE_DELTA_PATH}/raw"
}

BRONZE_PATHS = {
    "raw_pda_beneficiario": f"{BASE_DELTA_PATH}/bronze/raw_pda_beneficiario",
}

SILVER_PATHS = {
    "stg_cotacao_historica": f"{BASE_DELTA_PATH}/silver/stg_cotacao_historica",
    "stg_pda_beneficiario": f"{BASE_DELTA_PATH}/silver/stg_pda_beneficiario",
}

GOLD_PATHS = {
    "dim_ativo_financeiro": f"{BASE_DELTA_PATH}/gold/dim_ativo_financeiro",
    "dim_operadora": f"{BASE_DELTA_PATH}/gold/dim_operadora",
    "dim_municipio": f"{BASE_DELTA_PATH}/gold/dim_municipio",
    "dim_faixa_etaria": f"{BASE_DELTA_PATH}/gold/dim_faixa_etaria",
    "fato_cotacao": f"{BASE_DELTA_PATH}/gold/fato_cotacao",
    "fato_beneficiario": f"{BASE_DELTA_PATH}/gold/fato_beneficiario",
}