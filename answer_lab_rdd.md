Question 1: What decides the number of partitions textFile() picks by default, for a file this size?

textFile(path, minPartitions=None) uses a default minimum number of partitions of min(defaultParallelism, 2), exposed as sc.defaultMinPartitions. Spark asks the underlying Hadoop input format to split the file into chunks of about file size / minPartitions, but never larger than the block size (128 MB on HDFS). The two files here are tiny (about 7 KB and 331 KB), far below one block, so the minimum is what decides: both get 2 partitions, whatever the number of cores. For a large file, the block size takes over and Spark creates roughly one partition per block, so a 1 GB file would give about 8 partitions.

Question 2: repartition() and coalesce() both change the partition count. Which one shuffles data, and which one doesn’t?

repartition(n) always performs a full shuffle: it is coalesce(n, shuffle=True) underneath. It can increase or decrease the number of partitions, and it redistributes the rows evenly, which also fixes skewed partitions. coalesce(n) does not shuffle by default: it merges existing partitions on the same executor (a narrow dependency), so it is much cheaper, but it can only decrease the number of partitions and can leave them unbalanced. In practice, repartition() is used to increase parallelism or rebalance the data, and coalesce() to reduce the number of partitions cheaply, for example before writing fewer output files. The plans above show it: ShuffledRDD appears only for repartition().

Question 3: Call orders_data.count() a second time. Does the Spark UI show a new stage? Why?

Yes. The Spark UI shows a new job with its own stage, and the stage is executed again, not skipped. Each action triggers a new job, and Spark does not remember the result of a previous action. orders_data is not cached, so Spark replays the whole lineage: it reads the file again, applies the filter again, and counts again. (A stage would only appear as “skipped” in the UI if it could reuse shuffle files written by an earlier job. There is no shuffle here, so nothing can be reused.)

Question 4: At which point in this notebook would .cache() change that answer?

Calling .cache() on orders_data before the first count() would change it. cache() is lazy: nothing is stored when it is called, the partitions are kept in memory when the first action computes them. The second count() would then read them from memory instead of reading and filtering the file again. A new job would still appear, because every action creates a job, but its stage would read the cached data, run much faster, and the Storage tab of the Spark UI would list the cached RDD. Caching is worth it for an RDD reused by several actions, which is why the lab caches orders_rdd later, in the parsing cell.

Question 5: Why does spark.read.csv(multiLine=True) not have this problem, while sc.textFile() does?

textFile() splits the file on every newline character before any parsing happens. It has no knowledge of CSV, quoting or fields, so a newline inside a quoted address is treated as the end of a record, which cuts one user into two elements. The DataFrame CSV reader with multiLine=True runs a real CSV parser over the data, which keeps track of whether it is inside quotes: a newline inside quotes is then part of the field value, not a record separator. This has a price: with multiLine=True a file cannot be split into chunks read in parallel, because a reader starting in the middle of a file cannot know whether it is inside quotes. Each file is read by a single task, whereas a line-oriented reader can split a file anywhere, at a line boundary.

Question 6: In general terms, what has to be true of a file format for a line-oriented reader like textFile() to be safe to use on it?

Each record must be exactly one physical line, and every line must be understandable on its own:

The newline character must never appear inside a record: field values cannot contain raw line breaks (they must be escaped, as in JSON Lines, where a newline inside a string is written \n, or removed).
A record must not depend on the neighbouring lines: no records spread over several lines, and no state carried from one line to the next, apart from a header that is handled explicitly.
The fields must be separable without a stateful parser, for example with a delimiter that never appears inside values.

orders.csv, JSON Lines files and typical log files meet these conditions. CSV files with quoted multiline fields, pretty-printed JSON and XML do not.


Question 7: What would happen to this cell if one line in orders.csv had an extra comma inside product?

line.split(",") would return 6 fields instead of 5, so the unpacking into five variables would raise ValueError: too many values to unpack (expected 5). Because of lazy evaluation, nothing fails when the map is defined or when cache() is called: the error only appears when an action first processes that line (take(3) would only hit it if the line were among the first rows, otherwise it would appear at count() or later). The exception is raised inside a Python worker, Spark retries the task (up to spark.task.maxFailures, which is 1 in local[*] mode), and then aborts the whole job with a traceback. A single malformed row therefore fails the entire computation, and the failure appears far from the place where the faulty code was written. Worse, a malformed row that still has five fields, for example with a shifted column, would not raise anything and would silently produce wrong data.

Question 8: Where does the DataFrame API perform the equivalent check, and when?

The check is done by the CSV reader, when it parses each line according to a schema, either inferred (inferSchema, which costs an extra pass over the data at read time) or declared explicitly with a StructType. Like everything in Spark it is lazy, so the data is parsed when an action runs. What is different from the RDD code is that the behaviour is configurable through the read mode: PERMISSIVE (the default) keeps malformed rows with NULL values and can store the raw line in a _corrupt_record column, DROPMALFORMED silently drops them, and FAILFAST raises an error on the first bad record. The structure of the query (unknown column names, wrong types in expressions) is also validated earlier, at analysis time, before any data is read. With RDDs, none of this exists: the validation logic must be written by hand, and by default one bad row crashes the job.


Question 9: reduceByKey combines values on each partition before shuffling. What does groupByKey shuffle instead, and why is that more expensive?

Measured on this dataset: the reduceByKey stage wrote 2,513 bytes to the shuffle, against 6,230 bytes for groupByKey, about 2.5 times more, for an identical result. The record counters are the same (8) for both, because PySpark sends batches of pickled pairs to the JVM, so only the byte volume reflects the difference.

groupByKey does not reduce anything: it moves every value to the partition that owns its key, where the values are then collected into a list. In the Scala and Java APIs, this means shuffling every (key, value) pair as is. PySpark first groups the values by key inside each partition, so it sends fewer records, but all the values still travel, as lists. The shuffle volume is therefore proportional to the number of values, whereas with reduceByKey it is proportional to the number of distinct keys times the number of partitions, since each partition sends at most one partial result per key. It is also more expensive in memory: the reducer has to hold all the values of a key at once, so a key with a huge number of values (a skewed key) can run out of memory. Less data to write to disk, send over the network and read back is also why reduceByKey is faster.

Question 10: For which aggregations would groupByKey be unavoidable, reduceByKey not being able to express them?

reduceByKey needs an associative and commutative function that merges two partial results of the same type as the values. groupByKey is needed when the result cannot be built from small mergeable partial results and requires all the values of a key at once. Typical cases are an exact median or percentile, the mode, sorting or reordering the values of each key (for example rebuilding an ordered sequence of events per user), or applying an arbitrary function to a whole group (such as a custom model per key). An average, on the other hand, does not need groupByKey: it can be computed with (sum, count) pairs, which are mergeable, which is what aggregateByKey is for (exercise 1). In practice, aggregateByKey or combineByKey with a list as accumulator can also replace groupByKey, and groupByKey remains the simplest way to write the cases above.


Question 11: partitionBy() in the window function is described as a wide transformation, shuffling rows so every row of a given order_id lands on the same partition. What plays that role here?

The shuffle triggered by reduceByKey plays that role. After the map has turned each order into an (order_id, order) pair, reduceByKey hash-partitions the pairs by key, so all the pairs with the same order_id end up on the same partition, where they are merged with most_recent. It is the equivalent of PARTITION BY order_id. There is a difference in cost: reduceByKey first combines the values inside each partition, so only one candidate order per key and per partition crosses the network. A window function has to shuffle all the rows, then sort them inside each partition (the ORDER BY ordered_at DESC), and only then number them.

Question 12: Which version states the intent more directly: the window function, or the reduceByKey with a custom combiner?

The window function. row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) = 1 is declarative: it states what is wanted (the latest row for each order) and leaves the engine free to choose how to do it, which also lets the optimizer improve the execution. The key, the ordering and the selection can all be read directly. The reduceByKey version describes how: build key-value pairs, merge them with a function, then drop the keys. The intent (keeping the most recent row) is hidden inside a comparison of date strings, and the correctness relies on implicit assumptions: the combiner must be associative and commutative, the dates must be comparable as text, and ties are not handled explicitly. In exchange, it makes the data movement explicit, and the local combining before the shuffle can make it cheaper.


Question 13: join() is a wide transformation on both RDDs. What would need to be true of how orders_kv and users_kv are partitioned for Spark to skip the shuffle?

The two RDDs would have to be co-partitioned: partitioned by the same key (user_id), with the same partitioner (the same partition function) and the same number of partitions. Then rows with the same key are already on the partition with the same number in both RDDs, so the join becomes a narrow transformation and no data has to move. In practice, this is achieved by calling partitionBy(n) with the same n on both RDDs before the join, and persisting them if they are reused. It does not remove the cost, it moves it: the shuffle is paid once, at partitionBy, and it only pays off when the partitioned RDD is reused by several joins. If only one side is already partitioned, Spark only needs to shuffle the other one. A different way to avoid the shuffle altogether is a broadcast (map-side) join: when one side is small, like users_kv with its 50 rows, it can be sent to every executor and looked up in a map, with no shuffle at all.

Question 14: Replace join() with leftOuterJoin(). What changes in the result if a user_id in orders has no matching user?

With join() (an inner join), an order without a matching user is silently dropped from the result. With orders_kv.leftOuterJoin(users_kv), every order is kept, and the unmatched ones appear as (user_id, (order, None)): the username is None because there is nothing to match. The result then has at least as many rows as orders_kv, and the orphan orders can be found with a filter on the elements whose username is None. This is a convenient way to detect data quality problems, which is what exercise 2 will practise.

Question 15: enriched is not yet filtered or aggregated. What is the risk of calling .collect() on it directly, on a dataset much larger than this one?

collect() brings every element of the RDD back to the driver process. On a large dataset, the result must fit in the memory of the driver, which is a single machine, so the driver can run out of memory and crash. Spark also protects itself with spark.driver.maxResultSize (1 GB by default): above that, the job is aborted with an error. All the data also has to be serialised and sent over the network to one point, so the distributed processing is lost at the last step. Safer options are take(n) or sample() to look at a few rows, filtering or aggregating first so that the result is small, toLocalIterator() to read one partition at a time, or writing the result out in a distributed way instead of bringing it to the driver.