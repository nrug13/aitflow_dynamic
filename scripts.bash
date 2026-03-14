cd /home/nurgun/Documents/homework_mar_14

mkdir -p airflow/dags/scripts
mkdir -p airflow/files
mkdir -p airflow/logs
mkdir -p airflow/config
mkdir -p airflow/plugins
mkdir -p potgres/pg_files
cp test.py airflow/dags/scripts/test_etl.py
docker build -t custom-airflow:latest .
docker build -t new-airflow:2.10.5 .
docker compose up -d
docker exec -it minio mc alias set local http://localhost:9000 matrix matrix123 ##we are setting up an alias for the MinIO server with the name "local", the URL, and the access key and secret key.
docker exec -it minio mc mb local/lakehouse ##we are creating a new bucket named "lakehouse" in the MinIO server using the alias we just set up.
docker exec -it postgres psql -U airflow -d airflow
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100),
    amount DECIMAL(10,2),
    order_date DATE
);
INSERT INTO orders (customer_name, amount, order_date) VALUES
('Ali', 150.00, '2025-01-15'),
('Veli', 230.50, '2025-02-20'),
('Aysel', 89.99, '2025-03-10');
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    category VARCHAR(50)
);
INSERT INTO products (name, price, category) VALUES
('Laptop', 2500.00, 'Electronics'),
('Mouse', 25.00, 'Electronics'),
('Notebook', 5.00, 'Stationery');
docker exec -it airflow-scheduler spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/delta-spark_2.12-3.1.0.jar,/opt/spark/jars/delta-storage-3.1.0.jar,/opt/spark/jars/antlr4-runtime-4.9.3.jar,/opt/spark/jars/postgresql-42.7.3.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
  /opt/airflow/dags/scripts/test_etl.py \
  --postgres_table orders \
  --s3_path s3a://lakehouse/bronze/orders \
  --etl_date 2025-06-01


docker exec -it airflow-scheduler spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/delta-spark_2.12-3.1.0.jar,/opt/spark/jars/delta-storage-3.1.0.jar,/opt/spark/jars/antlr4-runtime-4.9.3.jar,/opt/spark/jars/postgresql-42.7.3.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
  /opt/airflow/dags/scripts/test_etl.py \
  --postgres_table products \
  --s3_path s3a://lakehouse/bronze/products \
  --etl_date 2025-06-01


  chmod -R 777 airflow/logs