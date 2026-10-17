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

