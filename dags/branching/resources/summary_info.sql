CREATE TABLE IF NOT EXISTS information_table AS
SELECT 'raw.character' AS table_name, count(1) AS record_count FROM raw.character
UNION ALL
SELECT 'raw.location' AS table_name, count(1) AS record_count FROM raw.location