#This notebook will contain the initialization of spark session..
from pyspark.sql import SparkSession

def getSparkSession():
    spark = SparkSession.builder.\
    appName('Ecom analysis').\
    master('local[2]').\
    getOrCreate()
    return spark


