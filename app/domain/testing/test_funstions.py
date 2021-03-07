from pyspark.sql import DataFrame


# function to spark dataframe equivalence test
def are_two_dataframes_equal(df1: DataFrame, df2: DataFrame ) -> bool:
    """function to spark dataframe equivalence test
    :param df1: pyspark dataframe 1
    :param df2: pyspark dataframe 2
    :return: True or False.
    """
    if df1.schema != df2.schema:
        return False
    elif df1.collect() != df2.collect():
        return False
    else:
        return True

