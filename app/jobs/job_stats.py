from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from app.domain.hirer import udf_job_stats as js


def _extract_advertiser(spark: SparkSession,
                        config: dict,
                        env: str) -> DataFrame:
    """Load advertiser data from json file format.
    :param spark: Spark session object.
    :param config: Static configurations for the job.
    :param env: environment prod/dev.
    :return: Spark DataFrame.
    """
    advertiser: DataFrame = spark.read.json(config["jobs.conf"][env]["source_data"]["advertiser"])

    return advertiser


def _extract_jobs(spark: SparkSession,
                  config: dict,
                  env: str) -> DataFrame:
    """Load jobs data from json file format.
    :param spark: Spark session object.
    :param config: Static configurations for the job.
    :param env: environment prod/dev.
    :return: Spark DataFrame.
    """
    jobs: DataFrame = spark.read.json(config["jobs.conf"][env]["source_data"]["jobs"])

    return jobs


def _transform_job_listing_stats(df_jobs: DataFrame) -> DataFrame:
    """Transform advertiser & jobs dataframe and computes jobs stats.
    :param df_jobs: jobs data
    :return: Spark DataFrame.
    """
    # transform using dataframe operation
    df_job_listing_stats = (df_jobs
                            .withColumn('is_engineering_ad', js.is_engineering_ad('job_title'))
                            .withColumn('is_entry_level_ad', js.is_entry_level_ad('job_level'))
                            .select('job_id', 'advertiser_id', 'is_engineering_ad', 'is_entry_level_ad')
                            )

    return df_job_listing_stats


def _transform_job_advertiser_stats(spark: SparkSession,
                                    df_advertiser: DataFrame,
                                    df_job_listing_stats: DataFrame) -> DataFrame:
    """Transform advertiser & jobs dataframe and computes advertiser stats.
    :param spark: Spark session object.
    :param df_advertiser: advertiser data
    :param df_job_listing_stats: jobs listing stats data
    :return: Spark DataFrame.
    """
    # register tables from dataframe
    df_advertiser.createOrReplaceTempView("vw_advertiser")
    df_job_listing_stats.createOrReplaceTempView("vw_job_listing_stats")
    # transform using sql operation
    df_job_advertiser_stats = spark.sql(
        """
        select ad.advertiser_name, 
               sum(js.is_entry_level_ad) as total_entry_level_ads,
               sum(js.is_engineering_ad) as total_engineering_ads
          from vw_job_listing_stats js,
               vw_advertiser ad
         where js.advertiser_id = ad.advertiser_id
         group by ad.advertiser_name
        """
    )
    return df_job_advertiser_stats


def _save_job_listing_stats(config: dict,
                            env: str,
                            df_job_listing_stats: DataFrame):
    """Saves job_listing_stats dataframe as json file format..
    :param config: Static configurations for the job.
    :param env: environment prod/dev.
    """
    (df_job_listing_stats
     .repartition(config["jobs.conf"][env]["npartition"])
     .write.mode("overwrite")
     .json(config["jobs.conf"][env]["output_data_location"]["job_listing_stats"]))


def _save_job_advertiser_stats(config: dict,
                               env: str,
                               df_job_advertiser_stats: DataFrame):
    """Saves job_advertiser_stats dataframe as json file format.
    :param config: Static configurations for the job.
    :param env: environment prod/dev.
    """
    (df_job_advertiser_stats
     .repartition(config["jobs.conf"][env]["npartition"])
     .write.mode("overwrite")
     .json(config["jobs.conf"][env]["output_data_location"]["job_advertiser_stats"]))


def run_job(spark: SparkSession, config: dict, env: str):
    """Triggers job_advertiser_stats pipeline
    :param spark: Spark session object.
    :param config: Static configurations for the job.
    :param env: environment prod/dev.
    """
    # register SQL UDFs
    js.register_udf_is_engineering_ad(spark)
    js.register_udf_is_entry_level_ad(spark)

    # extract required dataframes
    df_advertiser = _extract_advertiser(spark, config, env)
    df_jobs = _extract_jobs(spark, config, env)

    # perform transformation & save final outputs
    df_job_listing_stats = _transform_job_listing_stats(df_jobs)
    df_job_listing_stats.persist() # persist it as it will be used again
    _save_job_listing_stats(config, env, df_job_listing_stats)

    df_job_advertiser_stats = _transform_job_advertiser_stats(spark, df_advertiser, df_job_listing_stats)
    _save_job_advertiser_stats(config, env, df_job_advertiser_stats)
