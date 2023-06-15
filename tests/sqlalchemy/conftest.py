# -*- coding: utf-8 -*-
from urllib.parse import quote_plus

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa
from sqlalchemy.testing.plugin.pytestplugin import (
    pytest_sessionstart as sqlalchemy_pytest_sessionstart,
)
from sqlalchemy.testing.provision import (
    configure_follower,
    create_db,
    drop_db,
    temp_table_keyword_args,
)

from tests import ENV, SQLALCHEMY_CONNECTION_STRING


def pytest_sessionstart(session):
    conn_str = (
        SQLALCHEMY_CONNECTION_STRING + "&tblproperties=" + quote_plus("'table_type'='ICEBERG'")
    )
    session.config.option.dburi = [
        conn_str.format(
            region_name=ENV.region_name,
            schema_name=ENV.schema,
            s3_staging_dir=ENV.s3_staging_dir,
            location=ENV.s3_staging_dir,
            work_group=ENV.work_group,
        )
    ]
    sqlalchemy_pytest_sessionstart(session)


@create_db.for_db("awsathena")
def _awsathena_create_db(cfg, eng, ident):
    with eng.begin() as conn:
        try:
            _awsathena_drop_db(cfg, conn, ident)
        except Exception:
            pass

    with eng.begin() as conn:
        conn.exec_driver_sql(f"CREATE DATABASE {ident}")
        conn.exec_driver_sql(f"CREATE DATABASE {ident}_test_schema")
        conn.exec_driver_sql(f"CREATE DATABASE {ident}_test_schema_2")


@drop_db.for_db("awsathena")
def _awsathena_drop_db(cfg, eng, ident):
    with eng.begin() as conn:
        conn.exec_driver_sql(f"DROP DATABASE {ident} CASCADE")
        conn.exec_driver_sql(f"DROP DATABASE {ident}_test_schema CASCADE")
        conn.exec_driver_sql(f"DROP DATABASE {ident}_test_schema_2 CASCADE")


@configure_follower.for_db("awsathena")
def _awsathena_configure_follower(config, ident):
    config.test_schema = f"{ident}_test_schema"
    config.test_schema_2 = f"{ident}_test_schema_2"


@temp_table_keyword_args.for_db("awsathena")
def _awsathena_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["TEMPORARY"]}
