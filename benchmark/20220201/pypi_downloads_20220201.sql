CREATE EXTERNAL TABLE file_downloads_20220201 (
  timestamp BIGINT,
  country_code STRING,
  url STRING,
  project STRING,
  file STRUCT<
    filename: STRING,
    project: STRING,
    version: STRING,
    type: STRING
  >,
  details STRUCT<
    installer: STRUCT<
      name: STRING,
      version: STRING
    >,
    python: STRING,
    implementation: STRUCT<
      name: STRING,
      version: STRING
    >,
    distro: STRUCT<
      name: STRING,
      version: STRING,
      id: STRING,
      libc: STRUCT<
        lib: STRING,
        version: STRING
      >
    >,
    system: STRUCT<
      name: STRING,
      release: STRING
    >,
    cpu: STRING,
    openssl_version: STRING,
    setuptools_version: STRING
  >,
  tls_protocol STRING,
  tls_cipher STRING
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/path/to/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
