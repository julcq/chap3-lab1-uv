Exercice 1 :

the code : 
user_stats = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .aggregateByKey(
        (0, 0),                                    # zero value: (order_count, total_quantity)
        lambda acc, q: (acc[0] + 1, acc[1] + q),   # seqOp: adds one order inside a partition
        lambda a, b: (a[0] + b[0], a[1] + b[1]),   # combOp: merges the partial results of two partitions
    )
)
user_stats.take(5)

user_avg = user_stats.mapValues(lambda s: s[1] / s[0])
user_avg.take(5)

the result : 
[('f143262f-dc5c-4eed-8da0-365bf89897b9', 3.0), ('969b6662-0562-4059-968c-c69b1064005c', 3.3333333333333335), ('ce9e1a11-fcbb-4e59-bbdd-cf7c9c96e9ec', 2.976470588235294), ('5563f616-00e8-4ece-8b49-452d46d483f3', 2.7142857142857144), ('f94d6204-6808-493f-9fed-2c43e256a6dc', 3.5714285714285716)]

aggregateByKey takes a zero value and two functions: seqOp folds one value into 
the accumulator inside a partition, and combOp merges the accumulators coming from 
different partitions. Unlike reduceByKey, the accumulator type (here a (count, sum) pair) 
can differ from the value type (an integer). This gives both the order count and the total 
quantity in one pass over the data and one shuffle, whereas two separate reduceByKey 
calls would need two passes and two shuffles. The average cannot be computed by 
averaging partial averages, which is wrong when partitions have different sizes: 
the mergeable (count, sum) pair has to be kept, and the division is done at the end with mapValues.

----------------------------------------------------------------------------------------------------
Exercice 2 :

the code : 
fake = {
    "order_id": "fake-order-1",
    "user_id": "00000000-0000-0000-0000-000000000000",
    "date": "2020-01-01 00:00:00",
    "quantity": 1,
    "product": "bread",
}
orders_with_orphan = orders_rdd.union(sc.parallelize([fake]))
orders_kv_orphan = orders_with_orphan.map(lambda o: (o["user_id"], o))

joined = orders_kv_orphan.leftOuterJoin(users_kv)
orphans = joined.filter(lambda kv: kv[1][1] is None)

print(joined.count())
print(orphans.count())
print(orphans.collect())
print(orders_kv_orphan.join(users_kv).count())

the result : 
2830                                                                            
1
[('00000000-0000-0000-0000-000000000000', ({'order_id': 'fake-order-1', 'user_id': '00000000-0000-0000-0000-000000000000', 'date': '2020-01-01 00:00:00', 'quantity': 1, 'product': 'bread'}, None))]
2829  

RDDs are immutable, so union creates a new RDD and the original orders_rdd is left untouched. 
With leftOuterJoin, the fake order is kept and paired with None, which gives 2830 rows, and 
filtering on None isolates exactly one orphan order. With an inner join, the same order is 
silently dropped and the result stays at 2829 rows, so the data quality problem would go 
unnoticed. leftOuterJoin followed by a filter on None is a simple way to audit referential integrity.

-----------------------------------------------------------------------------------------------------
Exercice 3 : 

the code : 
orders_df = (
    spark.read.csv("orders.csv", header=True, inferSchema=True)
    .withColumnRenamed("user_uuid", "user_id")
)
orders_df.printSchema()

user_quantity_df = orders_df.groupBy("user_id").sum("quantity")
user_quantity_df.show(5)
user_quantity_df.explain()

the result : 
root
 |-- uuid: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- date: timestamp (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- product: string (nullable = true)

+--------------------+-------------+                                            
|             user_id|sum(quantity)|
+--------------------+-------------+
|d47d577b-fa5a-41c...|           80|
|0b2f6d5c-700b-4d5...|          190|
|d9e604b3-8412-433...|          184|
|97fa7f04-8363-400...|          285|
|2051acef-097a-4e1...|          301|
+--------------------+-------------+
only showing top 5 rows
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[user_id#23], functions=[sum(quantity#20)])
   +- Exchange hashpartitioning(user_id#23, 200), ENSURE_REQUIREMENTS, [plan_id=85]
      +- HashAggregate(keys=[user_id#23], functions=[partial_sum(quantity#20)])
         +- Project [user_uuid#18 AS user_id#23, quantity#20]
            +- FileScan csv [user_uuid#18,quantity#20] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/onyxia/work/chap3-lab1-uv/orders.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<user_uuid:string,quantity:int>

Compare with the version RDD : 

The code : 
print(user_quantity.toDebugString().decode())

the result : 
(2) PythonRDD[39] at collect at <python-input-21>:1 []
 |  MapPartitionsRDD[32] at mapPartitions at PythonRDD.scala:170 []
 |  ShuffledRDD[31] at partitionBy at NativeMethodAccessorImpl.java:0 []
 +-(2) PairwiseRDD[30] at reduceByKey at <python-input-19>:3 []
    |  PythonRDD[29] at reduceByKey at <python-input-19>:3 []
    |  PythonRDD[26] at RDD at PythonRDD.scala:58 []
    |      CachedPartitions: 2; MemorySize: 168.0 KiB; DiskSize: 0.0 B
    |  orders.csv MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0 []
    |  orders.csv HadoopRDD[2] at textFile at NativeMethodAccessorImpl.java:0 []


Line count. The RDD version needs about 15 lines to go from the raw file to the totals: 
remove the header, write a parse_order function, convert the lines, build key-value pairs, 
then call reduceByKey. The DataFrame version needs two statements, one to read the file 
and one for groupBy("user_id").sum("quantity"). The schema (names and types) comes from 
the header and from inferSchema, at the price of an extra pass over the file; an explicit 
StructType avoids that pass.

Plans. toDebugString() shows a chain of RDDs (PythonRDD, PairwiseRDD, ShuffledRDD) whose 
Python lambdas are opaque to Spark: it can only run them as written. explain() shows a 
plan optimised by Catalyst, which understands the semantics of the query. It contains two 
HashAggregate operators, a partial_sum before the Exchange (the shuffle) and a final one 
after it, which is the same map-side combine that reduceByKey performs, chosen automatically. 
The FileScan reads only the two columns needed (ReadSchema lists user_id and quantity), 
while the RDD version reads and parses all five fields of every line. The DataFrame aggregation 
also runs entirely in the JVM, without sending each row to a Python worker, which makes it faster.

--------------------------------------------------------------------------------------------------
Exercice 4 :

WITH .cache()

the code : 
for _ in range(3):
    timed("cached  orders_rdd.count()", lambda: orders_rdd.count())

for _ in range(3):
    timed("cached  user_quantity.count()", lambda: make_user_quantity(orders_rdd).count())

the result : 
cached  orders_rdd.count(): 2829 in 0.390s
cached  orders_rdd.count(): 2829 in 0.161s
cached  orders_rdd.count(): 2829 in 0.116s
cached  user_quantity.count(): 50 in 0.424s
cached  user_quantity.count(): 50 in 0.377s
cached  user_quantity.count(): 50 in 0.588s

FOR COMPARISON : 
orders_rdd.cache()
print(orders_rdd.count())

the result : 
2829


WITHOUT .cache() : 

the code : 
orders_rdd.unpersist()

for _ in range(3):
    timed("no cache  orders_rdd.count()", lambda: orders_rdd.count())

for _ in range(3):
    timed("no cache  user_quantity.count()", lambda: make_user_quantity(orders_rdd).count())

the result :
no cache  orders_rdd.count(): 2829 in 0.133s
no cache  orders_rdd.count(): 2829 in 0.183s
no cache  orders_rdd.count(): 2829 in 0.160s
no cache  user_quantity.count(): 50 in 0.253s
no cache  user_quantity.count(): 50 in 0.392s
no cache  user_quantity.count(): 50 in 0.503s

FOR COMPARISON : 
orders_rdd.cache()
print(orders_rdd.count())

the result : 
2829


The file contains only 2,829 lines, so the execution times are very short and dominated 
by the fixed overhead of a job (scheduling, starting up Python workers). The first run 
in each series is the slowest, which is why three repetitions are performed. Consequently, 
the difference between "with cache" and "without cache" will be modest—on the order of a 
few tens of milliseconds—or perhaps even lost within the noise.
With the cache, orders_rdd.count() reads the already parsed records from memory. Without 
it, every action replays the whole lineage: it reads orders.csv again, filters out the 
header and re-runs parse_order in a Python worker for each line. On this tiny dataset the 
difference in time is small, because the fixed overhead of scheduling a job and starting 
the Python workers dominates, and the first run of each series is slower because of warm-up. 
The effect is clearer on the Storage information of the Spark UI (or its REST API): with 
the cache, the RDD is listed with its cached partitions and the memory it uses, whereas 
after unpersist() nothing is listed. The benefit of .cache() grows with the size of the 
data, the cost of the parsing, and the number of actions that reuse the RDD, which is why 
it is worth it in the parsing cell, whose result is reused by the aggregation, 
deduplication and join sections.
