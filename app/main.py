import json
import argparse
from pyspark.sql import SparkSession
from app.dependencies import init_spark
from app.jobs import job_stats


def _parse_arguments():
    """ Parse arguments provided by spark-submit commend"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True)
    return parser.parse_args()


def main():
    """ Main function executed by spark-submit command"""

    # boilerplate codes
    with open("configs.json", "r") as config_file:
        config: dict = json.load(config_file)

    spark: SparkSession = init_spark.create_spark_session_using_configs_file()
    print(spark.sparkContext.getConf().getAll())

    # start pipeline job
    job_stats.run_job(spark, config, 'dev')


if __name__ == "__main__":
    main()
