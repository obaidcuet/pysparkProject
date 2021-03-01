from pyspark.sql import SparkSession, DataFrame
from app.domain import udf_job_advertiser_stats as jas


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
    advertiser.show()

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
    jobs.show()

    return jobs


def _transform_job_advertiser_stats(spark: SparkSession,
                                    df_advertiser: DataFrame,
                                    df_jobs: DataFrame) -> DataFrame:
    """Transform advertiser & jobs dataframe.
    :param spark: Spark session object.
    :param df_advertiser: advertiser data
    :param df_jobs: jobs data
    :return: Spark DataFrame.
    """
    # register tables from dataframe
    df_advertiser.createOrReplaceTempView("vw_advertiser")
    df_jobs.createOrReplaceTempView("vw_jobs")
    df_job_advertiser_stats = spark.sql(
        """
        select tx_jobad.advertiser_name, 
               sum(tx_jobad.is_entry_level_ad) as total_entry_level_ads,
               sum(tx_jobad.is_engineering_ad) as total_engineering_ads
          from (
            select 
                a.advertiser_name,
                is_entry_level_ad(j.job_level) as is_entry_level_ad,
                is_engineering_ad(j.job_title) as is_engineering_ad
              from vw_advertiser a, vw_jobs j
             where a.advertiser_id = j.advertiser_id
         ) tx_jobad
         group by tx_jobad.advertiser_name
        """
    )

    return df_job_advertiser_stats


def _save_job_advertiser_stats(spark: SparkSession,
                               config: dict,
                               env: str,
                               df_job_advertiser_stats: DataFrame):
    """Saves job_advertiser_stats dataframe as json file format.
    :param spark: Spark session object.
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
    jas.register_udf_is_engineering_ad(spark)
    jas.register_udf_is_entry_level_ad(spark)

    # extract required dataframes
    df_advertiser = _extract_advertiser(spark, config, env)
    df_jobs = _extract_jobs(spark, config, env)

    # perform transformation
    df_job_advertiser_stats = _transform_job_advertiser_stats(spark, df_advertiser, df_jobs)

    # save the output
    _save_job_advertiser_stats(spark, config, env, df_job_advertiser_stats)

