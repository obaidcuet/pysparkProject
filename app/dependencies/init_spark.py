import json
from pyspark.sql import SparkSession
from pyspark import SparkConf

# create spark session based on parameters in configs.json file
def create_spark_session_using_configs_file() -> SparkSession:
    with open("configs.json", "r") as config_file:
        config: dict = json.load(config_file)

    # create spark conf with all spark conf values in configs.json file
    spark_conf: SparkConf = SparkConf()
    for key in config['spark.conf']:
        spark_conf.set(key, config['spark.conf'][key])

    # create the spark session
    spark: SparkSession = (SparkSession.builder.
                           appName(config.get("app_name")).
                           config(conf=spark_conf).
                           enableHiveSupport().
                           getOrCreate())

    return spark
