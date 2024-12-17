import pytest
from pyspark.sql import SparkSession#, SparkConf, SparkContext
#sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local"))

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("chispa") \
      .getOrCreate()