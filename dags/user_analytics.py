import os
import shutil
from datetime import datetime, timedelta
import boto3
import duckdb
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

def download_s3_folder(s3_bucket: str, s3_folder: str, local_folder: str = "/opt/airflow/temp/s3folder/") -> None:
    """
    Descarga una carpeta completa de S3 a un directorio local.

    Args:
        s3_bucket (str): Nombre del bucket S3.
        s3_folder (str): Ruta de la carpeta en S3.
        local_folder (str, optional): Directorio local donde se descargará la carpeta. Por defecto es "/opt/airflow/temp/s3folder/".
    """
    # Configurar recurso S3
    s3 = boto3.resource(
        service_name="s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
        region_name="us-east-1",
    )
    bucket = s3.Bucket(s3_bucket)
    local_path = os.path.join(local_folder, s3_folder)

    # Eliminar la carpeta local si existe
    if os.path.exists(local_path):
        shutil.rmtree(local_path)

    # Descargar archivos del bucket S3 al directorio local
    for obj in bucket.objects.filter(Prefix=s3_folder):
        target = os.path.join(local_path, os.path.relpath(obj.key, s3_folder))
        os.makedirs(os.path.dirname(target), exist_ok=True)
        bucket.download_file(obj.key, target)
        print(f"Downloaded {obj.key} to {target}")

def create_user_behavior_metrics() -> None:
    """
    Crea métricas de comportamiento del usuario uniendo datos de compras y reseñas de películas.
    """
    query = """
    WITH up AS (
      SELECT 
        * 
      FROM 
        '/opt/airflow/temp/s3folder/raw/user_purchase/user_purchase.csv'
    ), 
    mr AS (
      SELECT 
        * 
      FROM 
        '/opt/airflow/temp/s3folder/clean/movie_review/*.parquet'
    ) 
    SELECT 
      up.customer_id, 
      SUM(up.quantity * up.unit_price) AS amount_spent, 
      SUM(
        CASE WHEN mr.positive_review THEN 1 ELSE 0 END
      ) AS num_positive_reviews, 
      COUNT(mr.cid) AS num_reviews 
    FROM 
      up 
      JOIN mr ON up.customer_id = mr.cid 
    GROUP BY 
      up.customer_id
    """
    duckdb.sql(query).write_csv("/opt/airflow/data/behavior_metrics.csv")

# Definir el DAG
with DAG(
    "user_analytics_dag",
    description="Un DAG para extraer datos de usuarios y reseñas de películas para analizar su comportamiento",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    user_analytics_bucket = "user-analytics"

    # Crear bucket S3
    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket",
        bucket_name=user_analytics_bucket
    )

    # Cargar reseñas de películas a S3
    movie_review_to_s3 = LocalFilesystemToS3Operator(
        task_id="movie_review_to_s3",
        filename="/opt/airflow/data/movie_review.csv",
        dest_key="raw/movie_review.csv",
        dest_bucket=user_analytics_bucket,
        replace=True,
    )

    # Cargar compras de usuarios a S3
    user_purchase_to_s3 = SqlToS3Operator(
        task_id="user_purchase_to_s3",
        sql_conn_id="postgres_default",
        query="SELECT * FROM retail.user_purchase",
        s3_bucket=user_analytics_bucket,
        s3_key="raw/user_purchase/user_purchase.csv",
        replace=True,
    )

    # Clasificar reseñas de películas
    classify_movie_reviews = BashOperator(
        task_id="classify_movie_reviews",
        bash_command="python /opt/airflow/dags/scripts/spark/random_text_classification.py",
    )

    # Descargar reseñas de películas del S3 al almacén de datos
    download_movie_reviews_to_warehouse = PythonOperator(
        task_id="download_movie_reviews_to_warehouse",
        python_callable=download_s3_folder,
        op_kwargs={"s3_bucket": "user-analytics", "s3_folder": "clean/movie_review"},
    )

    # Descargar compras de usuarios del S3 al almacén de datos
    download_user_purchases_to_warehouse = PythonOperator(
        task_id="download_user_purchases_to_warehouse",
        python_callable=download_s3_folder,
        op_kwargs={"s3_bucket": "user-analytics", "s3_folder": "raw/user_purchase"},
    )

    # Generar métricas de comportamiento del usuario
    generate_user_behavior_metrics = PythonOperator(
        task_id="generate_user_behavior_metrics",
        python_callable=create_user_behavior_metrics,
    )

    # Generar el tablero de control
    markdown_path = "/opt/airflow/dags/scripts/dashboard/"
    quarto_cmd = f"cd {markdown_path} && quarto render {markdown_path}/dashboard.qmd"
    generate_dashboard = BashOperator(
        task_id="generate_dashboard",
        bash_command=quarto_cmd
    )

    # Definir dependencias del DAG
    create_s3_bucket >> [user_purchase_to_s3, movie_review_to_s3]
    user_purchase_to_s3 >> classify_movie_reviews >> download_user_purchases_to_warehouse
    movie_review_to_s3 >> download_movie_reviews_to_warehouse
    [download_user_purchases_to_warehouse, download_movie_reviews_to_warehouse] >> generate_user_behavior_metrics >> generate_dashboard