>> PySpark tutorial
Spark Session
This is a Pyspark.sql package acts as an entry point to start spark. This is a class from where you can access the instance 
of Spark context. The Spark session is used to create DataFrame, register Dataframe as a table, execute SQL commands over it. 
Cache tables and read parquet files

 > Spark Context
It is the class of PySpark Package. It represents the connection to a spark cluster and cluster manager. All spark processes are indenpendent
processes coordinated by Spark Context in the driver programs
 It is used to create RDD and Broadcast variables on that cluster

The Application Driver has Spark Session and Spark context 

Cluster Manager: There are 5 types of cluster managers that is supported by spark
1)Standalone - Sparks cluster manager in its own environment
2)Mesos - The distributed Systems Kernel
3)Hadoop YARN
4)Kubernetes
5)Amazon EC2

Spark Data Abstractions 
1) RDD - Resilient Distributed Dataset RDD[T] - Immuatble set of distributed elements
2) DATAFRAME (Similar to Relational tables)

Examples of RDD
RDD[String] - Each element is a string
RDD[Integer] - Each element is an integer
RDD[(string, Integer)] - Each element is a pair of (String, Integer)

> Spark Operations
1) Transformations - converts source RDD into target RDD
2) Actions - Transforms the Source RDD into non-RDD object

SparkContext.Parallelize() - Creates a new RDD from Collections

rdd = sc.parallelize(data)
rdd.collect()
rdd.count()


> Steps in Executing Pyshell Spark

1) Enter into Pyspark shell
2) Create RDD from collections
rdd = sc.parallelize(data)
rdd.collect()
rdd.count()

3) Aggregate and Merge values of keys 
sum = rdd.reduceBykEY(Lambda x,y: x+y)
ReduceByKey() transformation is used to merge and aggregate and aggregate values

4) Filter RDD's Element
Sumfiltered = sum.filter(lambda(k,v): v>9)
sumfiltered.collect()

5) Group similar keys
grouped = rdd.groupByKey()
grouped.collect()
grouped.map(lamdba (k,v) : (k, list(v))).collect())

6) Aggregate values for similar keys 
aggregated = grouped.mapValues(lambda values :sum(values))
aggregated.collect()
ReducebyKey()
GroupBykey()

Refer the below link for pyspark documentation
> https://spark.apache.org/docs/latest/api/python/index.html


RDD[(string, Integer)] - RDD can be any datatype. The keys can be of any data type like string, integer.The key or value can be tuple.
The key is string and value is the tuple of elements in the example ("Ananya", (10,20))
Grouing on the keys.

Maps - The source RDD has n elements. if we apply function f on each and every element of the source RDD
rdd = 
rdd2 =rdd. map(lambda x : 3*x)

Def mult3():
return 3*x
end def
rdd 3 = rdd.map(mult3)
flatmap - takes list of elements and flattens all the elements
flatMap, Map are transformations
reduceby and groupby acts on keys
mapvalues- similar to mapper but only works on values 
reducebykey is very efficient as it uses combiner. The most efficient transformation.
groupbykey - should be used cautiously
for median then use groupbykey




