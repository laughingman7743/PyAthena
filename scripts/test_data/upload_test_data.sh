#!/bin/bash -xe

# TODO Upload to a unique S3 location for each test run
#aws s3 rm ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/one_row/ --recursive
#aws s3 rm ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/one_row_complex/ --recursive
#aws s3 rm ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/many_rows/ --recursive
#aws s3 rm ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/integer_na_values/ --recursive
#aws s3 rm ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/boolean_na_values/ --recursive
aws s3 cp $(dirname $0)/one_row.tsv ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/one_row/one_row.tsv
aws s3 cp $(dirname $0)/one_row_complex.tsv.gz ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/one_row_complex/one_row_complex.tsv.gz
aws s3 cp $(dirname $0)/many_rows.tsv ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/many_rows/many_rows.tsv
aws s3 cp $(dirname $0)/integer_na_values.tsv ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/integer_na_values/integer_na_values.tsv
aws s3 cp $(dirname $0)/boolean_na_values.tsv ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena/boolean_na_values/boolean_na_values.tsv
