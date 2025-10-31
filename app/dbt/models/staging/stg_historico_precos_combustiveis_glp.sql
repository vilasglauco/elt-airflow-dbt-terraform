-- Model to process and prepare historical data of GLP fuel prices.
-- This model extracts data from the source 'anp.historico_precos_combustiveis_glp', filters for the GLP product,
-- performs data cleaning and transformation, generates a surrogate key for unique identification,
-- removes duplicates keeping the most recent entry, and prepares the data for further analysis.

{{
    config(
        materialized='incremental',
        unique_key='sk_revenda_coleta'
    )
}}

WITH source AS (
    -- Select data from the source table, filtering only for the 'GLP' product
    SELECT * 
    FROM {{ source('anp', 'historico_precos_combustiveis_glp') }}
    WHERE "Produto" = 'GLP'
),

renamed_and_casted AS (
    -- Rename columns, clean data (TRIM strings and convert empty strings to NULL), 
    -- cast types for consistency (DATE, DECIMAL, TIMESTAMP), 
    -- normalize numeric fields by replacing ',' with '.' and using TRY_CAST to handle invalid values,
    -- and standardize text fields by removing non-numeric characters where appropriate
    SELECT
        -- Generate a surrogate key for each row based on CNPJ, Data da Coleta, and Produto.
        -- This key uniquely identifies each revenda-coleta-product combination for deduplication.
        {{ dbt_utils.generate_surrogate_key(['"CNPJ da Revenda"', '"Data da Coleta"', '"Produto"']) }} AS sk_revenda_coleta,
        TRIM("Regiao - Sigla") AS regiao_sigla,
        TRIM("Estado - Sigla") AS estado_sigla,
        TRIM("Municipio") AS municipio,
        NULLIF(TRIM("Revenda"), '') AS nome_revenda,
        -- Remove any non-numeric characters from CNPJ and trim spaces to standardize the identifier
        REGEXP_REPLACE(TRIM("CNPJ da Revenda"), '[^0-9]', '', 'g') AS cnpj_revenda,
        NULLIF(TRIM("Bandeira"), '') AS bandeira,
        NULLIF(TRIM("Nome da Rua"), '') AS nome_rua,
        NULLIF(TRIM("Numero Rua"), '') AS numero_rua,
        NULLIF(TRIM("Complemento"), '') AS complemento,
        NULLIF(TRIM("Bairro"), '') AS bairro,
        -- Ensure CEP has only digits; empty or whitespace-only strings become NULL
        REGEXP_REPLACE(NULLIF(TRIM("Cep"), ''), '[^0-9]', '', 'g') AS cep,
        TRIM("Produto") AS produto,
        TRIM("Unidade de Medida") AS unidade_medida,
        "Data da Coleta"::DATE AS data_coleta,
        -- Convert string to decimal, replacing ',' with '.'; TRY_CAST prevents errors for invalid values
        TRY_CAST(REPLACE(TRIM("Valor de Venda"), ',', '.') AS DECIMAL(10, 2)) AS valor_venda,
        TRY_CAST(REPLACE(TRIM("Valor de Compra"), ',', '.') AS DECIMAL(10, 2)) AS valor_compra,
        partition,
        source_file,
        ingestion_ts::TIMESTAMP AS ingestion_ts
    FROM source
),

deduplicated AS (
    -- Deduplicate records by keeping only the latest ingestion timestamp per surrogate key (sk_revenda_coleta)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY sk_revenda_coleta
            ORDER BY ingestion_ts DESC
        ) AS rn
        FROM (
            {% if is_incremental() %}
                -- For incremental runs, combine existing table data with new incoming records
                SELECT * FROM {{ this }}
                UNION ALL
                SELECT * FROM renamed_and_casted
            {% else %}
                -- For full refresh, select only the newly transformed data
                SELECT * FROM renamed_and_casted
            {% endif %}
        )
    )
)

-- Keep only the most recent record per surrogate key, removing duplicates and preparing the final dataset for analysis
-- This final filter ensures that only the record with row number 1 (the most recent ingestion) for each surrogate key is selected
-- Guarantees that downstream models always see the most recent ingestion for each revenda-coleta-product combination
SELECT
    sk_revenda_coleta,
    regiao_sigla,
    estado_sigla,
    municipio,
    nome_revenda,
    cnpj_revenda,
    bandeira,
    nome_rua,
    numero_rua,
    complemento,
    bairro,
    cep,
    produto,
    unidade_medida,
    data_coleta,
    valor_venda,
    valor_compra,
    partition,
    source_file,
    ingestion_ts
FROM deduplicated
WHERE rn = 1
