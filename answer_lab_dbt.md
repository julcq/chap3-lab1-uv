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

Question 3: The silver models are views. What happens when a view is queried, and when is it preferable to a table?

A view does not store any data, only its SQL query. Each time the view is queried, DuckDB expands its definition and runs the whole query again on the underlying sources. Here this means reading the CSV file on S3, parsing it, casting the columns and deduplicating with a window function, on every query.

A view is preferable when the result must always reflect the latest source data, when the transformation is light or the source is small, when the model is rarely queried, or when storage matters. It needs no refresh and no extra storage, which also makes it convenient during development.
A table is preferable when the transformation is expensive or the source is large or slow (a CSV on object storage), when the model is queried often (dashboards) or reused by many downstream models, or when a stable snapshot is needed. The trade-offs are extra storage and the need to rebuild it when the source changes.

Question 4: UUID values are stored in 16 bytes. How many bytes does the VARCHAR representation use?

The text form of a UUID has 36 characters: 32 hexadecimal digits and 4 hyphens. With one byte per ASCII character, that is 36 bytes of character data, more than twice the 16 bytes of the native UUID type. In memory, DuckDB also needs a fixed 16-byte header for each string, so a VARCHAR UUID takes about 52 bytes, roughly three times more, before any compression. This is why casting identifiers to UUID in the silver layer saves storage and speeds up joins and comparisons.


Question 5: Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region?

The right strategy depends on how the faulty field is used downstream.

Age dashboard: an invalid birthdate directly corrupts the metric (negative or unrealistic ages, a skewed average and distribution), so a warning is not enough. I would replace the invalid birthdate with NULL in the silver layer, and add a boolean column such as birthdate_is_valid to keep track of the correction. The age metrics then ignore the unknown values, the user and their orders are kept, and the dashboard can show the share of customers whose age is unknown. The producer should also be asked to fix the source.
Sales per region dashboard: the birthdate plays no role in this metric. The orders and the regions are valid, so rejecting the records would remove real sales and understate the revenue. I would keep all the records, accept the issue temporarily with a warning (severity='warn'), and monitor it until it is fixed at the source.

The principle is not to discard valid facts because of an unrelated bad attribute, and to clean or flag a value only where it would distort a result.

Question 6: The addresses associate states with random zip codes, KY 01352 for example. Which test would detect it, and which reference data would it require?

A consistency test between the state and the ZIP code would detect it, because the ZIP code must belong to the range of its state. 01352 is a Massachusetts ZIP code (the 010 to 027 range), while Kentucky codes are in the 400 to 427 range.

Reference data: a table mapping each state to its valid ZIP code ranges, or 3-digit ZIP prefixes, taken from an authoritative source such as the USPS. It can be loaded as a dbt seed or read as an external source.
Test: a singular test (or a reusable custom generic test) joins stg_users to this table and returns the users whose ZIP code falls outside the ranges of their state. It passes when no row is returned.
Caveats: the military regions (AA, AE, AP) have their own ZIP prefixes and need specific rows. A few ZIP codes also cross state borders. The reference data must be maintained because ZIP codes change over time.