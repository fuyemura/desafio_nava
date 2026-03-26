-- ============================================================
-- Análises — Beneficiários de Planos de Saúde (ANS/PDA)
-- Fonte: gold.fato_beneficiario
-- ============================================================

-- a) Quais são as 5 operadoras com maior número de beneficiários ativos
SELECT
    CD_OPERADORA,
    NM_RAZAO_SOCIAL,
    MODALIDADE_OPERADORA,
    SUM(QT_BENEFICIARIO_ATIVO) AS TOTAL_BENEFICIARIOS_ATIVOS
FROM gold.fato_beneficiario
GROUP BY
    CD_OPERADORA,
    NM_RAZAO_SOCIAL,
    MODALIDADE_OPERADORA
ORDER BY TOTAL_BENEFICIARIOS_ATIVOS DESC
LIMIT 5;

-- ------------------------------------------------------------

-- b) Qual a faixa etária com mais beneficiários e quantos são?
SELECT
    DE_FAIXA_ETARIA,
    SUM(QT_BENEFICIARIO_ATIVO) AS TOTAL_BENEFICIARIOS_ATIVOS
FROM gold.fato_beneficiario
GROUP BY
    DE_FAIXA_ETARIA
ORDER BY TOTAL_BENEFICIARIOS_ATIVOS DESC
LIMIT 1;

-- ------------------------------------------------------------

-- c) Liste, de forma decrescente, a quantidade de beneficiários por município
SELECT
    CD_MUNICIPIO,
    NM_MUNICIPIO,
    SG_UF,
    SUM(QT_BENEFICIARIO_ATIVO) AS TOTAL_BENEFICIARIOS_ATIVOS
FROM gold.fato_beneficiario
GROUP BY
    CD_MUNICIPIO,
    NM_MUNICIPIO,
    SG_UF
ORDER BY TOTAL_BENEFICIARIOS_ATIVOS DESC;
