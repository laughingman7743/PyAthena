#!/bin/bash -xe

aws s3 rm ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/one_row/one_row.tsv
aws s3 rm ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/one_row_complex/one_row_complex.tsv
aws s3 rm ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/many_rows/many_rows.tsv
aws s3 rm ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/integer_na_values/integer_na_values.tsv
