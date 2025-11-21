from pyspark import SparkContext;
import numpy as np;
from operator import add;

spark_contexto = SparkContext();

vetor = np.array([10, 20, 30, 40, 50]);
paralelo = spark_contexto.parallelize(vetor); #criando um RDD
print(paralelo); # resultado: ParallelCollectionRDD[3] at readRDDFromFile at PythonRDD.scala:274

mapa = paralelo.map(lambda x : x**2+x); #exemplo 10 * 10 + 10 
mapa.collect(); # [110, 420, 930, 1640, 2550]

lista_paralelo = spark_contexto.parallelize(['distribuida', 'distribuida', 'spark', 'rdd', 'spark', 'spark']);
funcao_lambda = lambda x:(x,1); # associa 1 a uma palavra

mapa2 = lista_paralelo.map(funcao_lambda).reduceByKey(add).collect();

for (w, c) in mapa2:
    print('{}:{}'.format(w,c));
    
lista = [1, 2, 3, 4, 5, 3];
lista_rdd = spark_contexto.parallelize(lista);    
lista_rdd.count();

par_ordenado = lambda numero : (numero, numero*10)

lista_rdd.flatMap(par_ordenado).collect(); # [1, 10, 2, 20, 3, 30, 4, 40, 5, 50, 3, 30]
lista_rdd.map(par_ordenado).collect(); # [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (3, 30)]

spark_contexto.stop();
