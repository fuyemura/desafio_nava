-- Carregar extensão Delta
LOAD delta;

-- Criar schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Mapear tabelas Delta
CREATE OR REPLACE VIEW bronze.raw_pda_beneficiario AS
SELECT * FROM delta_scan('D:/Projetos/desafio_nava/data/bronze/raw_pda_beneficiario');

CREATE OR REPLACE VIEW silver.stg_pda_beneficiario AS
SELECT * FROM delta_scan('D:/Projetos/desafio_nava/data/silver/stg_pda_beneficiario');

-- Dimensões Gold
CREATE OR REPLACE VIEW gold.dim_operadora AS
SELECT * FROM delta_scan('D:/Projetos/desafio_nava/data/gold/dim_operadora');

CREATE OR REPLACE VIEW gold.dim_municipio AS
SELECT * FROM delta_scan('D:/Projetos/desafio_nava/data/gold/dim_municipio');

CREATE OR REPLACE VIEW gold.dim_faixa_etaria AS
SELECT * FROM delta_scan('D:/Projetos/desafio_nava/data/gold/dim_faixa_etaria');

-- Fato Gold
CREATE OR REPLACE VIEW gold.fato_beneficiario AS
SELECT
    f.SG_UF,
    f.ID_CMPT_MOVEL,
    op.CD_OPERADORA,
    op.NM_RAZAO_SOCIAL,
    op.NR_CNPJ,
    op.MODALIDADE_OPERADORA,
    mun.CD_MUNICIPIO,
    mun.NM_MUNICIPIO,
    fe.DE_FAIXA_ETARIA,
    fe.DE_FAIXA_ETARIA_REAJ,
    f.QT_BENEFICIARIO_ATIVO,
    f.QT_BENEFICIARIO_ADERIDO,
    f.QT_BENEFICIARIO_CANCELADO,
    f.CRIADO_EM
FROM delta_scan('D:/Projetos/desafio_nava/data/gold/fato_beneficiario') f
LEFT JOIN delta_scan('D:/Projetos/desafio_nava/data/gold/dim_operadora')   op  ON f.SK_OPERADORA    = op.SK_OPERADORA
LEFT JOIN delta_scan('D:/Projetos/desafio_nava/data/gold/dim_municipio')   mun ON f.SK_MUNICIPIO    = mun.SK_MUNICIPIO
LEFT JOIN delta_scan('D:/Projetos/desafio_nava/data/gold/dim_faixa_etaria') fe ON f.SK_FAIXA_ETARIA = fe.SK_FAIXA_ETARIA;