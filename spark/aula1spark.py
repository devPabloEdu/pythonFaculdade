from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("TesteInstalaçãoSpark").getOrCreate();

dataset = spark.read.csv('/sample_data_california_housing_test.csv', inferSchema=True, header=True);
dataset.printSchema();