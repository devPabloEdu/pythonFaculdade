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

query1 = 'SELECT MAX(total_roms) as maximo_quartos FROM tabela_temporaria';
q_maximo_quartos = spark_session.sql(query1); #roda a query
panda_maximo_quartos = q_maximo_quartos.toPandas(); #converte a tabela para o formato do pandas
print('A quantidade máxima de quartos é: {}'.format(panda_maximo_quartos['maximo_quartos'])); #faz a interpolação com o format
quantidade_maxima_quartos = int(panda_maximo_quartos.loc[0,'maximo_quartos']); #converte para um inteiro

query2 = 'SELECT longitude, latitude FROM tabela_temporaria WHERE total_roms = '+str(q_maximo_quartos);
localizacao_maximo_quartos = spark_session.sql(query2);
pandas_localizacao_maximo_quartos = localizacao_maximo_quartos.toPandas();
print(pandas_localizacao_maximo_quartos.head());


