CREATE EXTERNAL TABLE pypi_downloads_20180915 (
  timestamp BIGINT,
  country_code STRING,
  url STRING,
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
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.literal'='
{
    "type": "record",
    "name": "Root",
    "fields": [{
        "name": "timestamp",
        "type": {
            "type": "long",
            "logicalType": "timestamp-micros"
        }
    }, {
        "name": "country_code",
        "type": ["null", "string"]
    }, {
        "name": "url",
        "type": "string"
    }, {
        "name": "file",
        "type": {
            "type": "record",
            "namespace": "root",
            "name": "File",
            "fields": [{
                "name": "filename",
                "type": "string"
            }, {
                "name": "project",
                "type": "string"
            }, {
                "name": "version",
                "type": "string"
            }, {
                "name": "type",
                "type": "string"
            }]
        }
    }, {
        "name": "details",
        "type": ["null", {
            "type": "record",
            "namespace": "root",
            "name": "Details",
            "fields": [{
                "name": "installer",
                "type": ["null", {
                    "type": "record",
                    "namespace": "root.details",
                    "name": "Installer",
                    "fields": [{
                        "name": "name",
                        "type": ["null", "string"]
                    }, {
                        "name": "version",
                        "type": ["null", "string"]
                    }]
                }]
            }, {
                "name": "python",
                "type": ["null", "string"]
            }, {
                "name": "implementation",
                "type": ["null", {
                    "type": "record",
                    "namespace": "root.details",
                    "name": "Implementation",
                    "fields": [{
                        "name": "name",
                        "type": ["null", "string"]
                    }, {
                        "name": "version",
                        "type": ["null", "string"]
                    }]
                }]
            }, {
                "name": "distro",
                "type": ["null", {
                    "type": "record",
                    "namespace": "root.details",
                    "name": "Distro",
                    "fields": [{
                        "name": "name",
                        "type": ["null", "string"]
                    }, {
                        "name": "version",
                        "type": ["null", "string"]
                    }, {
                        "name": "id",
                        "type": ["null", "string"]
                    }, {
                        "name": "libc",
                        "type": ["null", {
                            "type": "record",
                            "namespace": "root.details.distro",
                            "name": "Libc",
                            "fields": [{
                                "name": "lib",
                                "type": ["null", "string"]
                            }, {
                                "name": "version",
                                "type": ["null", "string"]
                            }]
                        }]
                    }]
                }]
            }, {
                "name": "system",
                "type": ["null", {
                    "type": "record",
                    "namespace": "root.details",
                    "name": "System",
                    "fields": [{
                        "name": "name",
                        "type": ["null", "string"]
                    }, {
                        "name": "release",
                        "type": ["null", "string"]
                    }]
                }]
            }, {
                "name": "cpu",
                "type": ["null", "string"]
            }, {
                "name": "openssl_version",
                "type": ["null", "string"]
            }, {
                "name": "setuptools_version",
                "type": ["null", "string"]
            }]
        }]
    }, {
        "name": "tls_protocol",
        "type": ["null", "string"]
    }, {
        "name": "tls_cipher",
        "type": ["null", "string"]
    }]
}
')
STORED AS AVRO
LOCATION 's3://YOUR_BUCKET/path/to/';
