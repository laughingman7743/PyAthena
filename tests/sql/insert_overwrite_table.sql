INSERT OVERWRITE TABLE {schema}.one_row_complex
SELECT
    true,
    127,
    32767,
    2147483647,
    9223372036854775807,
    0.5,
    0.25,
    'a string',
    cast('2017-01-01 00:00:00' as timestamp),
    cast('2017-01-02' as date),
    '123',
    array(1, 2),
    map(1, 2, 3, 4),
    named_struct('a', 1, 'b', 2),
    0.1
FROM (select 1) dual;
