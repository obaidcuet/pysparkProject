import json
import pytest

from pyspark.sql import SparkSession, DataFrame

from app.dependencies import init_spark
from app.jobs import job_stats


# read config
@pytest.fixture(scope='module')
def config():
    # -------------- setup ------------
    # boilerplate codes
    config_filename= '../../configs.json'
    with open(config_filename, "r") as config_file:
        config: dict = json.load(config_file)
    yield config # this will be used in module-wide in this module

# setup spark session
@pytest.fixture(scope='module')
def spark(config: dict):
    # -------------- setup ------------
    # boilerplate codes
    spark: SparkSession = init_spark.create_spark_session_using_configs_file(config, 'dev')
    # start pipeline job with dev env to generate output using dev data
    job_stats.run_job(spark, config, 'dev')
    yield spark # this will be used in module-wide in this module
    # ------------- teardown ----------
    spark.stop()


# tests job_listing_stats datasets
def test_job_listing_stats(spark: SparkSession,
                           config: dict):
    """tests job_listing_stats datasets
    :param config: Static configurations for the job.
    """
    df_test_job_listing_stats: DataFrame = spark.read.json(config["jobs.conf"]["dev"]["source_data"]["job_listing_stats"])
    df_output_job_listing_stats: DataFrame = spark.read.json(config["jobs.conf"]["dev"]["output_data_location"]["job_listing_stats"])

    assert df_test_job_listing_stats.schema == df_output_job_listing_stats.schema
    assert df_test_job_listing_stats.collect() == df_output_job_listing_stats.collect()


# tests job_advertiser_stats datasets
def test_job_advertiser_stats(spark: SparkSession,
                              config: dict):
    """tests job_advertiser_stats datasets
    :param config: Static configurations for the job.
    """
    df_test_job_advertiser_stats: DataFrame = spark.read.json(config["jobs.conf"]["dev"]["source_data"]["job_advertiser_stats"])
    df_output_job_advertiser_stats: DataFrame = spark.read.json(config["jobs.conf"]["dev"]["output_data_location"]["job_advertiser_stats"])

    assert df_test_job_advertiser_stats.schema == df_output_job_advertiser_stats.schema
    assert df_test_job_advertiser_stats.collect() == df_output_job_advertiser_stats.collect()
