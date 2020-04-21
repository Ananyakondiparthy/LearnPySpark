## PySpark tutorial
Spark Session
This is a Pyspark.sql package acts as an entry point to start spark. This is a class from where you can access the instance 
of Spark context. The Spark session is used to create DataFrame, register Dataframe as a table, execute SQL commands over it. 
Cache tables and read parquet files

##Spark Context
It is the class of PySpark Package. It represents the connection to a spark cluster and cluster manager. All spark processes are indenpendent
processes coordinated by Spark Context in the driver programs
> It is used to create RDD and Broadcast variables on that cluster

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

Spark Operations
1) Transformations - converts source RDD into target RDD
2) Actions - Transforms the Source RDD into non-RDD object

SparkContext.Parallelize() - Creates a new RDD from Collections

rdd = sc.parallelize(data)
rdd.collect()
rdd.count()


#Steps in Executing Pyshell Spark

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






