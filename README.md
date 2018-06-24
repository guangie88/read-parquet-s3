# `read-parquet-s3`

Example set-up to read Parquet files from S3 via Spark

## How to try

You will need Scala and SBT set up.

Change the configuration values in `src/main/resources/application.conf`, and
then run:

```bash
sbt run
```

If correctly set up, you should not see any error messages, and a number of
entries from the Parquet file in S3 should be shown in console.
