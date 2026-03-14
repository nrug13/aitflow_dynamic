import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

parser = argparse.ArgumentParser()
parser.add_argument("--postgres_table", required=True)
parser.add_argument("--s3_path", required=True)
parser.add_argument("--etl_date", required=True)
# parser.add_argument("--pg_host", required=True)
# parser.add_argument("--pg_port", required=True)
# parser.add_argument("--pg_db", required=True)
# parser.add_argument("--pg_user", required=True)
# parser.add_argument("--pg_password", required=True)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("etl_pipeline")
    .config(
        "spark.jars",
        ",".join([
            "/opt/spark/jars/delta-spark_2.12-3.1.0.jar",
            "/opt/spark/jars/delta-storage-3.1.0.jar",
            "/opt/spark/jars/antlr4-runtime-4.9.3.jar",
            "/opt/spark/jars/postgresql-42.7.3.jar",
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
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

jdbc_url = f"jdbc:postgresql://postgres:5432/airflow"

df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", args.postgres_table)
    .option("user", "airflow")
    .option("password", "airflow")
    .option("driver", "org.postgresql.Driver")
    .load()
)

df = df.withColumn("etl_date", lit(args.etl_date))

df.show()

(
    df.repartition(5).write.format("delta")
    .mode("overwrite")
    .save(args.s3_path)
)

spark.stop()