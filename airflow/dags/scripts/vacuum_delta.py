import argparse
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

parser = argparse.ArgumentParser()
parser.add_argument("--delta_path", required=True, help="S3a path to delta table")
parser.add_argument("--retention_hours", required=True, type=int, help="Retention period in hours")
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("delta_vacuum")
    .config(
        "spark.jars",
        ",".join([
            "/opt/spark/jars/delta-spark_2.12-3.1.0.jar",
            "/opt/spark/jars/delta-storage-3.1.0.jar",
            "/opt/spark/jars/antlr4-runtime-4.9.3.jar",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar",
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
        ])
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "matrix")
    .config("spark.hadoop.fs.s3a.secret.key", "matrix123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

if args.retention_hours < 168:
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

delta_table = DeltaTable.forPath(spark, args.delta_path)

print(f"Running VACUUM on {args.delta_path} with retention = {args.retention_hours} hours")
delta_table.vacuum(retentionHours=args.retention_hours)
print("VACUUM completed successfully")

spark.stop()
