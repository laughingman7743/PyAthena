import packaging.version
import sqlalchemy

sqlalchemy_1_4_or_more = packaging.version.parse(sqlalchemy.__version__) >= packaging.version.parse(
    "1.4"
)
