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

DROP TABLE IF EXISTS {schema}.partition_table;
CREATE EXTERNAL TABLE IF NOT EXISTS {schema}.partition_table (
    a STRING
)
PARTITIONED BY (b INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
LOCATION '{location_partition_table}';

DROP TABLE IF EXISTS {schema}.integer_na_values;
CREATE EXTERNAL TABLE IF NOT EXISTS {schema}.integer_na_values (
    a INT,
    b INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
LOCATION '{location_integer_na_values}';

DROP TABLE IF EXISTS {schema}.boolean_na_values;
CREATE EXTERNAL TABLE IF NOT EXISTS {schema}.boolean_na_values (
    a BOOLEAN,
    b BOOLEAN
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE
LOCATION '{location_boolean_na_values}';
