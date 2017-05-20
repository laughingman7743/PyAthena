DROP TABLE IF EXISTS {schema}.one_row;
CREATE EXTERNAL TABLE IF NOT EXISTS {schema}.one_row (number_of_rows INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
LOCATION '{location_one_row}';

DROP TABLE IF EXISTS {schema}.many_rows;
CREATE EXTERNAL TABLE IF NOT EXISTS {schema}.many_rows (
    a INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
LOCATION '{location_many_rows}';

DROP TABLE IF EXISTS {schema}.one_row_complex;
CREATE EXTERNAL TABLE IF NOT EXISTS {schema}.one_row_complex (
    col_boolean BOOLEAN,
    col_tinyint TINYINT,
    col_smallint SMALLINT,
    col_int INT,
    col_bigint BIGINT,
    col_float FLOAT,
    col_double DOUBLE,
    col_string STRING,
    col_timestamp TIMESTAMP,
    col_date DATE,
    col_binary BINARY,
    col_array ARRAY<int>,
    col_map MAP<int, int>,
    col_struct STRUCT<a: int, b: int>,
    col_decimal DECIMAL(10,1)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
LOCATION '{location_one_row_complex}';
