from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import Row, SQLContext
import numpy as np

def get_sparkcontext():
    conf = SparkConf()
    conf.setMaster('spark://spark-master:7077')
    conf.setAppName('spark-basic')
    sc = SparkContext(conf=conf)
    return sc

def mod(x):
    return (x, np.mod(x, 2))

def range_mod(sc, range_num=1000):
    rdd = sc.parallelize(range(range_num)).map(mod).take(1000)
    print(rdd)



if __name__ == "__main__":
    sc = get_sparkcontext()
    sqlContext = SQLContext(sc)
    l = [('Ankit',25),('Jalfaizy',22),('saurabh',20),('Bala',26)]
    rdd = sc.parallelize(l)
    people = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))
    schemaPeople = sqlContext.createDataFrame(people)
    print(type(schemaPeople))
    print(schemaPeople)
    print(f"head: {schemaPeople.head()}")
    print(f"count: {schemaPeople.count()}")
    schemaPeople.describe().show()
