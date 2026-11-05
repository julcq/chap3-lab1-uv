Exercice 1: 
the code :
users_pq = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
print(users_pq.count())
spark.sql("DROP TABLE IF EXISTS lab.silver.users PURGE")
users_pq.writeTo("lab.silver.users").create()
spark.sql("SELECT snapshot_id, operation FROM lab.silver.users.snapshots").show(truncate=False)

Excercice 2:
the code :
users = spark.table("lab.silver.users")
new_users = users.limit(3).withColumn("user_id", F.expr("uuid()")).withColumn("username", F.concat("username", F.lit("_b2")))
new_users.writeTo("lab.silver.users").append()
spark.sql("SELECT snapshot_id, operation, committed_at, summary['added-records'] AS added FROM lab.silver.users.snapshots ORDER BY committed_at").show(truncate=False)

I created lab.silver.users from the 50 silver users and appended a batch of 3 new users with new user_id values. The table now has two snapshots, both with operation append: the first adds 50 records and the second adds 3 records, which brings the table to 53 rows. The second snapshot has the first one as its parent. The original data file was not modified: the append wrote one small new file and a new manifest.

Exercice 3 :
the code :
users_snaps = spark.sql("SELECT snapshot_id FROM lab.silver.users.snapshots ORDER BY committed_at").collect()
first_users = users_snaps[0]["snapshot_id"]
spark.sql(f"SELECT count(*) AS before_append FROM lab.silver.users VERSION AS OF {first_users}").show()
spark.sql("SELECT count(*) AS after_append FROM lab.silver.users").show()

Querying the table with VERSION AS OF <first snapshot id> returns 50 users, the table as it was before the append. Querying the current table returns 53 users. Both queries read from the same immutable data files: the old version just uses the manifest list of the first snapshot, which references only the original file, so no copy of the data was needed.

Exercice 4 :
the code :
spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
more_users = spark.table("lab.silver.users").limit(2).withColumn("user_id", F.expr("uuid()")).withColumn("country", F.lit("US"))
more_users.writeTo("lab.silver.users").append()
spark.sql("SELECT country, count(*) AS users FROM lab.silver.users GROUP BY country").show()
spark.sql(f"SELECT * FROM lab.silver.users VERSION AS OF {first_users}").printSchema()

I added the column with ALTER TABLE ... ADD COLUMN country STRING, which only creates a new metadata version: no data file was rewritten. I then appended 2 users with country = 'US' using the evolved schema. The 53 existing users have country = NULL, because their files do not contain the column, and the 2 new users have US. Time travel to the first snapshot still returns the original 10-column schema without country, since each snapshot keeps the schema it was written with. This shows that old and new data coexist in one table without migration.

Exercice 5 :
the code : 
aws s3 --endpoint-url "$S3_ENDPOINT_URL" ls "s3://$LAB_BUCKET_NAME/warehouse/silver/users/" --recursive

Both tables have the same layout, because it is defined by Iceberg and not by the data. Each one has a data/ directory with immutable Parquet files and a metadata/ directory with one vN.metadata.json per change (data write or schema change), one manifest list (snap-*.avro) per snapshot, one manifest (*-m0.avro) per write, and version-hint.text. The differences are in the content. orders has the larger initial file (84 KB for 2829 rows) and users a much smaller one (about 7 KB for 50 rows). The number of files depends on the number of writes, not on the number of rows: each append or insert added one small data file and one manifest, and the ALTER TABLE added a metadata version without any data file. With many small appends this layout produces many small files, which is why Iceberg tables need periodic compaction.

Exercice 6:
Parquet is a file format. It stores rows in a columnar, compressed, typed way, with the schema and statistics in each file’s footer. It says nothing about how several files form a table. Iceberg is a table format built on top of such files. It adds metadata (schemas, snapshots, manifests with file statistics) that defines exactly which files make up the table at each version. This provides atomic commits, time travel, safe schema evolution and file skipping, and it works on files in object storage. An Iceberg catalog is the service or registry that knows the tables: it maps a name such as lab.silver.orders to the location of the table’s current metadata, and it performs the atomic switch of that pointer when a commit happens. This lets several engines and users share the same tables safely, with access control and consistent names. In short: Parquet holds the data, Iceberg describes the table, and the catalog tells every engine where the table is and arbitrates who may change it.
