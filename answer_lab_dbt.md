Question 1: The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation six months later?

Because the raw data is kept unchanged, the bronze layer is an immutable source of truth, and everything downstream can be rebuilt from it.

The bug is fixed once, in the SQL of the model, and the silver layer is rebuilt from the original files (with dbt build). No data has been lost or corrupted by the faulty transformation.
If the transformation had been applied before storing the data (the classic ETL approach), the original values would be gone. The only option would be to extract the data again from the source system, which may no longer hold it, or may now hold different values.
The raw files make the problem easier to investigate: the raw and transformed values can be compared side by side to understand exactly what went wrong and which records were affected.
The result is reproducible and auditable: the same input and the same code always give the same output, and the corrected pipeline can be replayed over the full history.

Question 2: The source reads the whole file on every query. Which other format and layout would you choose if the orders were ingested every hour for years?

I would store the orders as Parquet files, partitioned by date, instead of one growing CSV file.

Format: Parquet is columnar, typed and compressed. A query only reads the columns it needs, and the min/max statistics in each file allow whole row groups to be skipped, as seen in the DuckDB lab. This is far cheaper than parsing a text file in full on every query.
Layout: one directory per period in the Hive convention, for example bronze/orders/year=2026/month=10/day=24/, with each hourly batch written as a new, immutable file. Appending data never rewrites old files, and a query on a time range only reads the matching partitions.
Partition granularity: daily partitions are enough here. Hourly ones would create many small files, which is slow on object storage because of the per-request latency. Small files can also be compacted periodically into larger ones.
Incremental processing: with this layout, dbt can use incremental models that only process the partitions added since the last run, instead of recomputing the whole history every hour.
Table format (optional): an open table format such as Apache Iceberg on top of the Parquet files would add atomic commits, schema evolution, time travel and automatic compaction, which are valuable on a table that grows for years.

The raw data would still be kept as received. If the producer sends CSV, the original files can stay in the bronze layer, or be landed directly as Parquet if the format can be controlled at the source.