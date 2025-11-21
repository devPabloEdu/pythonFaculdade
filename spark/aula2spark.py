from pyspark import SparkContext;
from pyspark.sql import SparkSession;

spark_contexto = SparkContext();
print(spark_contexto);
print(spark_contexto.version());

spark_session = SparkSession.builder.getOrCreate();
print(spark_session);

dataSet = spark_session.read.csv('/spark/sample_data_california_housing_test.csv', inferSchema=True, header=True);
dataSet.head(); # imprime os nomes das colunas e os valores da primeira linha do csv/dataSet
dataSet.count(); # obtem a quantidade de linhas no DataSet

dataSet.createOrReplaceGlobalTempView('tabela_temporaria');
print(spark_session.catalog.listTables());

query = 'FROM tabela_temporaria SELECT longitude, latitude LIMIT 3';
saida = spark_session.sql(query);
saida.show();