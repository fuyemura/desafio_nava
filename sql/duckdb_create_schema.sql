-- Carregar extensão Delta
LOAD delta;

-- Criar schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Mapear tabelas Delta na camada Bronze
CREATE OR REPLACE VIEW bronze.raw_pda_beneficiario AS
SELECT * FROM delta_scan('D:/Projetos/desafio_nava/data/bronze/raw_pda_beneficiario');