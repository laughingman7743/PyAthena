.. _sqlalchemy:

SQLAlchemy
==========

Install SQLAlchemy with ``pip install "SQLAlchemy>=1.0.0"`` or ``pip install PyAthena[SQLAlchemy]``.
Supported SQLAlchemy is 1.0.0 or higher.

.. code:: python

    from sqlalchemy import func, select
    from sqlalchemy.engine import create_engine
    from sqlalchemy.sql.schema import Table, MetaData

    conn_str = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/"\
               "{schema_name}?s3_staging_dir={s3_staging_dir}"
    engine = create_engine(conn_str.format(
        aws_access_key_id="YOUR_ACCESS_KEY_ID",
        aws_secret_access_key="YOUR_SECRET_ACCESS_KEY",
        region_name="us-west-2",
        schema_name="default",
        s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/"))
    with engine.connect() as connection:
        many_rows = Table("many_rows", MetaData(), autoload_with=connection)
        result = connection.execute(select(func.count()).select_from(many_rows))
        print(result.scalar())

Connection string
-----------------

The connection string has the following format:

.. code:: text

    awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&...

If you do not specify ``aws_access_key_id`` and ``aws_secret_access_key`` using instance profile or boto3 configuration file:

.. code:: text

    awsathena+rest://:@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&...

Dialect & driver
----------------

+-----------+--------+------------------+----------------------+
| Dialect   | Driver | Schema           | Cursor               |
+===========+========+==================+======================+
| awsathena |        | awsathena        | DefaultCursor        |
+-----------+--------+------------------+----------------------+
| awsathena | rest   | awsathena+rest   | DefaultCursor        |
+-----------+--------+------------------+----------------------+
| awsathena | pandas | awsathena+pandas | :ref:`pandas-cursor` |
+-----------+--------+------------------+----------------------+
| awsathena | arrow  | awsathena+arrow  | :ref:`arrow-cursor`  |
+-----------+--------+------------------+----------------------+

Dialect options
---------------

Table options
~~~~~~~~~~~~~

location
    Type:
        str
    Description:
        Specifies the location of the underlying data in the Amazon S3 from which the table is created.
    value:
        s3://bucket/path/to/
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_location="s3://bucket/path/to/")
compression
    Type:
        str
    Description:
        Specifies the compression format.
    Value:
        * BZIP2
        * DEFLATE
        * GZIP
        * LZ4
        * LZO
        * SNAPPY
        * ZLIB
        * ZSTD
        * NONE|UNCOMPRESSED
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_compression="SNAPPY")
row_format
    Type:
        str
    Description:
        Specifies the row format of the table and its underlying source data if applicable.
    Value:
        * [DELIMITED FIELDS TERMINATED BY char [ESCAPED BY char]]
        * [DELIMITED COLLECTION ITEMS TERMINATED BY char]
        * [MAP KEYS TERMINATED BY char]
        * [LINES TERMINATED BY char]
        * [NULL DEFINED AS char]
        * SERDE 'serde_name'
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_row_format="SERDE 'org.openx.data.jsonserde.JsonSerDe'")
file_format
    Type:
        str
    Description:
        Specifies the file format for table data.
    Value:
        * SEQUENCEFILE
        * TEXTFILE
        * RCFILE
        * ORC
        * PARQUET
        * AVRO
        * ION
        * INPUTFORMAT input_format_classname OUTPUTFORMAT output_format_classname
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_file_format="PARQUET")
            Table("some_table", metadata, ..., awsathena_file_format="INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'")
serdeproperties
    Type:
        dict[str, str]
    Description:
        Specifies one or more custom properties allowed in SerDe.
    Value:
        .. code:: python

            { "property_name": "property_value", "property_name": "property_value", ... }
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_serdeproperties={
                "separatorChar": ",", "escapeChar": "\\\\"
            })
tblproperties
    Type:
        dict[str, str]
    Description:
        Specifies custom metadata key-value pairs for the table definition in addition to predefined table properties.
    Value:
        .. code:: python

            { "property_name": "property_value", "property_name": "property_value", ... }
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_tblproperties={
                "projection.enabled": "true",
                "projection.dt.type": "date",
                "projection.dt.range": "NOW-1YEARS,NOW",
                "projection.dt.format": "yyyy-MM-dd",
            })
bucket_count
    Type:
        int
    Description:
        The number of buckets for bucketing your data.
    Value:
        Integer value greater than or equal to 0
    Example:
        .. code:: python

            Table("some_table", metadata, ..., awsathena_bucket_count=5)

All table options can also be configured with the connection string as follows:

.. code:: text

    awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3%3A%2F%2Fbucket%2Fpath%2Fto%2F&location=s3%3A%2F%2Fbucket%2Fpath%2Fto%2F&file_format=parquet&compression=snappy&...

``serdeproperties`` and ``tblproperties`` must be converted to strings in the ``'key'='value','key'='value'`` format and url encoded.
If single quotes are included, escape them with a backslash.

For example, if you configure a projection setting ``'projection.enabled'='true','projection.dt.type'='date','projection.dt.range'='NOW-1YEARS,NOW','projection.dt.format'= 'yyyy-MM-dd'`` in tblproperties, it would look like this

.. code:: text

    awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?s3_staging_dir=s3%3A%2F%2Fbucket%2Fpath%2Fto%2F&tblproperties=%27projection.enabled%27%3D%27true%27%2C%27projection.dt.type%27%3D%27date%27%2C%27projection.dt.range%27%3D%27NOW-1YEARS%2CNOW%27%2C%27projection.dt.format%27%3D+%27yyyy-MM-dd%27

Column options
~~~~~~~~~~~~~~

partition
    Type:
        bool
    Description:
        Specifies a key for partitioning data.
    Value:
        True / False
    Example:
        .. code:: python

            Column("some_column", types.String, ..., awsathena_partition=True)

partition_transform
    Type:
        str
    Description:
        Specifies a partition transform function for partitioning data.
        Only has an effect for ICEBERG tables and when partition is set to true for the column.
    Value:
        * year
        * month
        * day
        * hour
        * bucket
        * truncate
    Example:
        .. code:: python

            Column("some_column", types.Date, ..., awsathena_partition=True, awsathena_partition_transform='year')

partition_transform_bucket_count
    Type:
        int
    Description:
        Used for N in the bucket partition transform function, partitions by hashed value mod N buckets.
        Only has an effect for ICEBERG tables and when partition is set to true and
        when the partition transform is set to 'bucket' for the column.
    Value:
        Integer value greater than or equal to 0
    Example:
        .. code:: python

            Column("some_column", types.String, ..., awsathena_partition=True, awsathena_partition_transform='bucket', awsathena_partition_transform_bucket_count=5)

partition_transform_truncate_length
    Type:
        int
    Description:
        Used for L in the truncate partition transform function, partitions by value truncated to L.
        Only has an effect for ICEBERG tables and when partition is set to true and
        when the partition transform is set to 'truncate' for the column.
    Value:
        Integer value greater than or equal to 0
    Example:
        .. code:: python

            Column("some_column", types.String, ..., awsathena_partition=True, awsathena_partition_transform='truncate', awsathena_partition_transform_truncate_length=5)

cluster
    Type:
        bool
    Description:
        Divides the data in the specified column into data subsets called buckets, with or without partitioning.
    Value:
        True / False
    Example:
        .. code:: python

            Column("some_column", types.String, ..., awsathena_cluster=True)

To configure column options from the connection string, specify the column name as a comma-separated string.
The options partition_transform, partition_transform_bucket_count, partition_transform_truncate_length are not supported
to be configured from the connection string.

.. code:: text

    awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?partition=column1%2Ccolumn2&cluster=column1%2Ccolumn2&...

If you want to limit the column options to specific table names only, specify the table and column names connected by dots as a comma-separated string.

.. code:: text

    awsathena+rest://:@athena.us-west-2.amazonaws.com:443/default?partition=table1.column1%2Ctable1.column2&cluster=table2.column1%2Ctable2.column2&...

Temporal/Time-travel with Iceberg
---------------------------------

Athena supports time-travel queries on Iceberg tables by either a version_id or a timestamp. The `FOR TIMESTAMP AS OF`
clause is used to query the table as it existed at the specified timestamp. To build a time travel query by timestamp,
use `with_hint(table_name, "FOR TIMESTAMP AS OF timestamp)` after the table name in the SELECT statement, as in the
following example.

..code:: python

        select(table.c).with_hint(table_name, "FOR TIMESTAMP AS OF '2024-03-17 10:00:00'")

which will build a statement that outputs the following:

..code:: sql

        SELECT * FROM table_name FOR TIMESTAMP AS OF '2024-03-17 10:00:00'

To build a time travel query by version_id, use `with_hint(table_name, "FOR VERSION AS OF version_id")` after the table
name. Note: the version_id is also know as a snapshot_id can be retrieved by querying the `table_name$snapshots`
or `table_name$history` metadata. Again the hint goes after the select statement as in the following example.

..code:: python

        select(table.c).with_hint(table_name, "FOR VERSION AS OF 949530903748831860")

..code:: sql

        SELECT * FROM table_name FOR VERSION AS OF 949530903748831860
