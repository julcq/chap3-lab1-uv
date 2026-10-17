Question 1: The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on Onyxia?

Risks:

The secret is stored in plain text in ~/.duckdb/stored_secrets. Anyone or anything that can read this file gets the S3 credentials and can read or write the data.
The file can leak by mistake: copied folder, Git commit, Docker image, backup of the home directory, screenshot.
The credentials remain available as long as the file exists, even when they are no longer needed.

Why the risks are limited on Onyxia:

The credentials are temporary. The key starts with ASIA and comes with a session token: these are STS credentials which expire, so a leaked file quickly becomes useless.
They are scoped to the user’s own bucket, not to the whole storage.
The home directory belongs to a single-user, isolated pod, and it disappears with the service.

Question 2: The secret is visible to any process running as your user. How would you restrict the access to S3 for a Kubernetes Job?

Apply the principle of least privilege: the Job should not reuse the personal credentials of the user.

Give the Job dedicated credentials with a policy limited to the bucket or prefix it needs, and read-only if it only reads data.
Store them in a Kubernetes Secret, injected as environment variables or a mounted file, and protected by RBAC so that only the Job’s namespace and ServiceAccount can access it.
Prefer short-lived credentials (STS), which expire automatically.
In DuckDB, create a temporary secret (CREATE SECRET without PERSISTENT) when the Job starts, so nothing is written to disk. Its scope can be restricted with SCOPE 's3://bucket/prefix'.
Optionally, add a NetworkPolicy to restrict where the pod can connect.

Question 3: The dataset contains 50 users, each one with at least one order, and about 2800 orders with unique identifiers. Why does approx_unique return about 40 and about 3100?

approx_unique does not compute an exact count. It uses a probabilistic algorithm (HyperLogLog), which estimates the number of distinct values in a single pass, with a small and fixed amount of memory, without storing every value. The price is a margin of error: the result can be a few percent, sometimes more, away from the real value, which explains 40 instead of 50 and about 3100 instead of 2829. This trade-off is worth it on very large datasets, where an exact count(DISTINCT ...) would be slow and memory-hungry. When the exact number matters, as in a data quality check, use count(DISTINCT column).

Question 4: The types are inferred from a sample of the file. Which issue may occur with a large file whose first rows are not representative?

DuckDB only looks at a sample of the file (by default the first rows) to choose the type of each column. If the sample is not representative, the inferred type can be wrong. For example, a column that only contains integers in the sample is typed as BIGINT, but a later row contains text or a decimal value. The query then fails with a conversion error, or the data is silently altered (for instance, leading zeros lost in a postal code read as a number). Possible fixes are to increase the sample size (sample_size = -1 scans the whole file), to declare the types explicitly with the columns or types options, or to read everything as text with all_varchar = true and cast the columns later, typically in the silver layer.



Question 5: Why is the uuid column barely compressed?

The uuid values are random identifiers: each one is unique, so there is almost no redundancy to exploit.

Dictionary encoding, which works very well on user_uuid and product, is useless here: with 2829 distinct values out of 2829 rows, the dictionary would be as large as the data itself, so the writer falls back to plain encoding.
Snappy only looks for repeated byte sequences. It does not do entropy coding, so it cannot take advantage of the fact that UUIDs only use 16 hexadecimal characters.
The small gain (about 10%) comes from the few patterns that do repeat: the dashes at fixed positions and the 4-byte length prefix written before each string.

Question 6: Why is the compressed size of some columns larger than their uncompressed size?

This happens for date (22667 vs 22661 bytes) and product (1271 vs 1266 bytes).

These columns are already compact after encoding (binary timestamps, dictionary indexes), so the compression codec finds almost nothing left to remove.
Compression is not free: each compressed page carries a small overhead (headers and block framing). When the gain is smaller than this overhead, the compressed size ends up slightly larger.
The difference is negligible here (a few bytes), but it shows that compression is only worthwhile when the data has redundancy.

Question 7: Why is partitioning by uuid a bad idea? Refer to the limitations of object storage.

Partitioning by uuid creates one partition per distinct value, so about 2829 directories, each containing a file with a single row.

Object storage has no real directories: product=bread/ is only a prefix in the key name. Every partition is therefore a separate set of objects.
Listing objects is slow and paginated (1000 keys per request on S3), and DuckDB must list all these prefixes before it can read anything.
Every object access is an HTTP request with a high fixed latency. Reading thousands of tiny files means thousands of requests, and the latency dominates the actual data transfer.
Each Parquet file has its own footer, which can be larger than the single row it describes. The metadata overhead exceeds the data, and the columnar benefits (compression, column pruning) disappear.
Many small objects also increase the cost (per-request pricing) and the load on the storage metadata.

Partitioning only pays off on a column with low cardinality and, ideally, a column that queries filter on. A unique identifier is the opposite.

Question 8: Which partition column would you choose for a dataset of orders growing every day?

I would partition by a time-based column derived from the order date, typically year and month (year=2026/month=10).

Analytical queries almost always filter on a time range, so the engine can skip the partitions outside it.
Time grows in a predictable way: new orders go into new partitions, and old partitions never have to be rewritten.
The number of partitions stays bounded and grows slowly (12 per year), with files large enough to be efficient.
Retention is simple: old partitions can be deleted or moved to a cheaper storage class, for example with a lifecycle rule.

The granularity depends on the volume. Partitioning by day or hour only makes sense if each partition holds enough data (roughly hundreds of MB), otherwise it creates the small files problem again. In this lab, with one order per hour, even a daily partition would hold only 24 rows, so a monthly one is more reasonable. In DuckDB, this is written with PARTITION_BY (year(date), month(date)). Partitioning by product, as done in the lab, is fine to show the mechanism but is less suited to a growing dataset, because it does not help time-based queries and each partition grows without limit.


Question 9: How many row groups does the file contain, and how many are skipped by the filter date >= '2100-01-01'?

The file contains 3 row groups (123,573 + 124,276 + 4,567 = 252,416 rows). Their date ranges, taken from the footer statistics, are:

Row group 0: 2020-01-01 to 2034-02-04
Row group 1: 2034-02-04 to 2048-04-10
Row group 2: 2048-04-10 to 2048-10-17

The latest date in the whole file is 2048-10-17, which is earlier than 2100-01-01. The maximum of every row group is therefore below the filter value, so all 3 row groups are skipped. The query returns 0 without reading any column data. This matches the measurement: only 16.0 KiB were received in a single GET request, which is just the footer of the file. In the lab’s own run the dataset was larger and contained dates after 2100, so only some row groups were skipped there.

Question 10: The orders are sorted by date. What would happen to the efficiency of the filter if they were shuffled?

The filter would become much less efficient. Row group skipping relies on the min/max statistics stored in the footer, and these are only useful when each row group covers a narrow range of values.

With sorted data, each row group covers its own period, and the ranges do not overlap.
With shuffled data, every row group would contain dates from the whole period (2020 to 2048), so the min/max of each one would be almost identical.
The engine could then no longer prove that a row group has no matching row, so it would have to read the date column of all row groups.

Column pruning would still work, but row group skipping would be lost. This is why data is often sorted or clustered on the columns that queries filter on before it is written to Parquet.

Question 11: Compare the execution time of the CSV and Parquet queries. Which part of the difference is due to the network, which part to the parsing of the CSV file?

With the same aggregation (sum of quantity per product), the CSV query took 1.28 s and the Parquet query 0.444 s, so Parquet was about 3 times faster. The data volumes differ much more: 28.2 MiB for the CSV against 203.5 KiB for Parquet, about 140 times less.

Network part: the CSV query has to download the whole file, even though it only needs two columns out of five. The Parquet query reads the footer, then only the column chunks of product and quantity, which are highly compressed (dictionary encoding and small integers). Parquet uses more requests (4 GET against 1) but transfers far fewer bytes.
Parsing part: the CSV is text. DuckDB has to detect the dialect and the types, split every line, and convert the strings into integers and timestamps, for all rows and all columns it has to read. Parquet stores typed, encoded binary data, so it can be used almost directly, and the 3 unneeded columns are never touched.

The two parts cannot be separated with these figures alone. The platform network is fast here, so the gap in time is much smaller than the gap in volume. On a slower or busier network, the CSV query would suffer much more, because its cost grows with the 28.2 MiB downloaded, while the Parquet query would stay almost the same. To measure each part, you could time the download of the CSV (network cost), then query the local copy (parsing cost without network):
time aws s3 --endpoint-url "$S3_ENDPOINT_URL" cp "s3://$LAB_BUCKET_NAME/large/orders.csv" /tmp/orders_large.csv
duckdb -c ".timer on" -c "FROM read_csv('/tmp/orders_large.csv', strict_mode=false) SELECT product, sum(quantity) GROUP BY product"


