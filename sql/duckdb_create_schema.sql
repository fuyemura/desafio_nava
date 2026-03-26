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