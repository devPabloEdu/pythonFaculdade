import pandas as pd;
import numpy as np;
from pyspark import SparkContext;
from pyspark.sql import SparkSession;

spark_contexto = SparkContext();
spark_session = SparkSession.builder.getOrCreate();


media = 0;
desvio_padrao = 0.1;
pandas_temporario = pd.DataFrame(np.random.normal(media, desvio_padrao, 100));
spark_temporario = spark_session.createDataFrame(pandas_temporario);

print(spark_session.catalog.listTables());

spark_temporario.createOrReplaceTempView('nova_tabela_temporaria');

print(spark_session.catalog.listTables());

spark_session.stop();