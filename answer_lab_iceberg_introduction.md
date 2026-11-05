Question 1: Which files contain the actual rows?

Only the Parquet files in the data/ directory contain the rows. There is a single file, 00000-...-0-00001.parquet, holding all 2829 orders. Everything under metadata/ contains no row of the table.

Question 2: Which files describe the table?

The files in the metadata/ directory, plus version-hint.text. v1.metadata.json is the table metadata: it holds the schema, the partition spec, the table properties, the list of snapshots and which one is current. The snapshot file (snap-<id>-...avro) is the manifest list: it lists the manifests that make up that version of the table. The manifest file (<uuid>-m0.avro) lists the data files, with their path, record count, size and column statistics. version-hint.text contains the number of the current metadata version, so a reader knows which vN.metadata.json to open first.

Question 3: Why does an Iceberg table need more than the Parquet files?

Parquet files alone only describe themselves: each footer holds its own schema and statistics, and a folder of Parquet files has no notion of which files belong to the table. Iceberg adds the metadata that a table needs. It records exactly which files make up each version of the table, so a file is part of the table only if a manifest references it. It records the schema and its history, so columns can change without rewriting the files. It keeps snapshots, which provide atomic commits, isolation between readers and writers, and time travel. It also stores file-level statistics, which let the engine skip files without opening them. Without this layer, listing the directory is the only way to know the table content, which is slow on object storage and unsafe when files are being written or removed.

Question 4: How many snapshots does the table currently have?

One. The table was created with a single write (createOrReplace()), which produced one snapshot. Its operation is overwrite, because createOrReplace() is a “replace table as select”, so Iceberg records the content as replacing the table. Its parent_id is NULL because there is no previous version.

Question 5: How many data files belong to the current table?

One: a single Parquet file of 84,055 bytes containing all 2829 records. It matches the single file in the data/ directory. The 2829 rows were written in one partition, so Spark produced one file.

Question 6: What information does the snapshots table contain that the Parquet files do not?

The Parquet files only describe their own content. The snapshots table describes the history of the table: the ID of each version, when it was committed, which snapshot it came from (parent_id), the operation that produced it (append, overwrite, delete...), the manifest list that defines exactly which files belong to that version, and a summary with counters such as the number of added records and files and the total number of records. Parquet footers cannot provide any of this, because a file knows nothing about the other files, the versions or the operation that created it.

Question 7: How many snapshots are there now?

Two. The first one is the initial load of 2829 rows, created by createOrReplace(), with the operation overwrite. The second one is the append of 3 new orders, with the operation append. The second snapshot has the first one as its parent, so the table now has a history, and the row count is 2832.

Question 8: Did the original Parquet files disappear?

No. The original 84 KB Parquet file is still in data/, and the append only added a small new file with the 3 new rows. Iceberg never modifies data files: it writes new ones and creates new metadata that points to both the old and the new files. The first snapshot still references only the original file, and the current snapshot references both. The snapshot summary confirms it: the append added 1 data file with 3 records (1,952 bytes), and the table totals are now 2 data files, 2832 records and 86,007 bytes, with manifests-kept = 1, meaning the first manifest was reused.

Question 9: Why does Iceberg create a new snapshot instead of simply replacing the table?

Because each commit must be an atomic, immutable version of the table. Replacing the table in place would destroy the previous state, and a concurrent reader could see a half-written table. With a new snapshot, the new metadata file is written first, and the commit is the single step that makes it current. Readers already running keep a consistent view of the old snapshot, and new readers see the new one. Keeping the old snapshot also gives a history of the table, the possibility to query a past version (time travel), to roll back after a mistake and to audit what changed and when. The old files stay until snapshots are expired, which is a deliberate maintenance operation.

Question 10: What does time travel read?

It reads the table as it was at the chosen snapshot. Iceberg starts from the metadata of that snapshot: the manifest list of the snapshot, then the manifests, then only the data files those manifests reference. Here, VERSION AS OF <first snapshot> reads only the original 84 KB Parquet file (2829 rows), and ignores the small file added by the append. The current table reads both files (2832 rows). The query is a normal read, it just starts from a different snapshot.

Question 11: Why can Iceberg access an older version without making a complete copy of the table?

Because a snapshot is only metadata: a list of pointers to data files, not a copy of the data. Data files are immutable. A write creates new files and a new snapshot, and never modifies the existing files. Consecutive snapshots therefore share most of their files. In this lab the original file belongs to both snapshots, and the append added only 1,952 bytes of new data. Keeping an old version costs the small metadata files plus the data files that are no longer used by the current version, which is far cheaper than duplicating the whole table at every change.

Question 12: What would happen if the old snapshot’s metadata and data files were physically deleted?

Time travel to that snapshot would fail, because the snapshot or the files it points to would no longer exist (a “snapshot not found” or a “file not found” error). The consequences depend on what is deleted. Removing the old snapshot through Iceberg’s expire_snapshots maintenance procedure is safe: it only deletes files that no remaining snapshot references. Deleting files by hand is dangerous when they are shared. In this lab the original 84 KB data file is used by both snapshots, so deleting it would also break the current table, with a “file not found” error on reads. The metadata still describes files that are gone, and nothing in object storage prevents this, so such deletions must go through Iceberg’s own maintenance operations.

Question 13: Did the existing Parquet files have to be rewritten?

No. ALTER TABLE ... ADD COLUMN is a metadata-only operation: it creates a new metadata file that contains the new schema, and it does not touch any data file. The existing files do not contain the source column, and Iceberg returns NULL for it when it reads them. This is safe because Iceberg identifies columns by a unique ID and not by name or position. The files table confirms that the two original data files are unchanged, and only one new file was written by the INSERT.

Question 14: Where is the new schema recorded?

In the table metadata JSON file. The ALTER TABLE created a new version, v3.metadata.json, with a new entry in its schemas list (a new schema-id that includes the source field and its column ID), and current-schema-id points to it. The older schema is kept in the same list, and each snapshot knows which schema it was written with. version-hint.text is updated to point to the new metadata version.

Question 15: Why is schema evolution a table-level operation rather than simply changing a Parquet file?

A table is made of many Parquet files, written at different times, and all of them must be read through one consistent schema. Changing the footer of a single file would make the files disagree with each other, and rewriting every file for each change would be slow, expensive and not atomic. By recording the schema in the table metadata, with a unique ID per column, Iceberg makes the change instant, atomic and independent of the data volume. Old files stay valid, readers see the same schema, and old snapshots still read correctly with the schema they were written with, so time travel is not broken.

Question 16: Which one is simply a collection of Parquet files?

silver/spark/orders.parquet/. It is a directory with a Parquet part-file and a _SUCCESS marker. Spark treats it as a table only because it lists the directory and reads whatever files it finds. Nothing in it records which files belong together, what the schema is or what the previous content was.

Question 17: Which one has table metadata and snapshots?

warehouse/silver/orders/. Next to the data/ directory with the Parquet files, its metadata/ directory holds the versioned table metadata (v1 to v4.metadata.json), the manifest lists and manifests in Avro, and version-hint.text. These record the schema and its history, the snapshots, and exactly which data files belong to each version.

Question 18: How would you safely replace a Parquet directory while another query is reading it?

Never delete and rewrite the directory in place. The safe approach is to write the new content to a new location, for example a versioned directory such as orders.parquet/version=2/ or a staging path, and only when the write is complete switch readers to it with a single small update: a pointer file, a view, or the location in a metastore. The old directory is kept until the running queries have finished, and is deleted afterwards. Renaming a directory on object storage is not atomic (it is a copy then a delete), so a rename is not a safe way to switch. This pattern is a manual version of what a table format like Iceberg does with snapshots.

Question 19: How does Iceberg make the table state visible atomically?

Iceberg writes the new data files first, then the new manifests and a new metadata file, and none of them is visible to readers at this stage because nothing points to them. The commit is a single step: the pointer to the current metadata is switched to the new version (version-hint.text here, or an atomic update in a catalog). A reader resolves the current metadata once, at the beginning of its query, and then works on that snapshot, whose data files are immutable. It therefore sees either the old table or the new one, never a mix. If a job fails before the final switch, its files stay unreferenced and are ignored, and the table is unchanged.

Question 20: Why is a catalog needed once several engines need to share the same Iceberg table?

The catalog is the shared place that answers “where is the current metadata of this table?”. Without one, each engine must know the path and rely on conventions such as version-hint.text. With several engines writing, they need an agreed, atomic way to commit: the catalog updates the pointer with a compare-and-swap, so if two engines commit at the same time, one is rejected and retries on top of the new version, instead of silently overwriting the other. It also provides table names (namespace.table) that every engine resolves the same way, discovery of tables, and central access control. The file-based hadoop catalog used in this lab has none of these guarantees on object storage, so it is fine for a lab but not for several concurrent writers. In production a catalog service such as Polaris, a REST catalog or Hive Metastore plays this role.