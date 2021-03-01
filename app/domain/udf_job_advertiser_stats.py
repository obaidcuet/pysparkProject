from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
import re


def _is_entry_level_ad(job_level: str) -> int:
    """parse job_level to identify whether it is entry level job
    :param job_level: Job level from job dataset
    :return: if entry level job then 1, else 0
    """
    if any(re.findall(r'JUNIOR|ASSOCIATE', job_level, re.IGNORECASE)):
        return 1
    else:
        return 0


def _is_engineering_ad(job_title: str) -> int:
    """parse job_title to identify whether it is engineering job
    :param job_title: Job title from job dataset
    :return: if engineering job then 1, else 0
    """
    if any(re.findall(r'ENGINEER', job_title, re.IGNORECASE)):
        return 1
    else:
        return 0


# register functions for SQL operation
def register_udf_is_entry_level_ad(spark: SparkSession):
    """Registers UDF is_entry_level_ad for SQL usage
    :param spark: Spark session object.
    """
    spark.udf.register("is_entry_level_ad", _is_entry_level_ad, IntegerType())


# register functions for SQL operation
def register_udf_is_engineering_ad(spark: SparkSession):
    """Registers UDF is_engineering_ad for SQL usage
    :param spark: Spark session object.
    """
    spark.udf.register("is_engineering_ad", _is_engineering_ad, IntegerType())


# function for dataframe operation
is_entry_level_ad = udf(_is_entry_level_ad, IntegerType())
is_engineering_ad = udf(_is_engineering_ad, IntegerType())
