from pyspark.sql import SparkSession, DataFrame
from app.domain.testing import test_funstions as tf


# tests job_listing_stats datasets
def test_job_listing_stats(spark: SparkSession,
                           config: dict):
    """tests job_listing_stats datasets
    :param config: Static configurations for the job.
    """
    df_test_job_listing_stats: DataFrame = spark.read.json(config["jobs.conf"]["dev"]["test_data_location"]["job_listing_stats"])
    df_output_job_listing_stats: DataFrame = spark.read.json(config["jobs.conf"]["dev"]["output_data_location"]["job_listing_stats"])

    assert tf.are_two_dataframes_equal(df_test_job_listing_stats, df_output_job_listing_stats)


# tests job_advertiser_stats datasets
def test_job_advertiser_stats(spark: SparkSession,
                              config: dict):
    """tests job_advertiser_stats datasets
    :param config: Static configurations for the job.
    """
    df_test_job_advertiser_stats: DataFrame = spark.read.json(config["jobs.conf"]["dev"]["test_data_location"]["job_advertiser_stats"])
    df_output_job_advertiser_stats: DataFrame = spark.read.json(config["jobs.conf"]["dev"]["output_data_location"]["job_advertiser_stats"])

    assert tf.are_two_dataframes_equal(df_test_job_advertiser_stats, df_output_job_advertiser_stats)

