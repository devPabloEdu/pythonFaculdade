from pyspark.sql import SparkSession

# cria uma sessão do spark
spark = SparkSession.builder.master("local[*]").appName("TesteInstalaçãoSpark").getOrCreate();

dataset = spark.read.csv('/sample_data_california_housing_test.csv', inferSchema=True, header=True);
dataset.printSchema();

#finaliza a sessão do spark
spark.stop();