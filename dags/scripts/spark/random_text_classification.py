import argparse
from pyspark.ml.feature import StopWordsRemover, Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, lit


def random_text_classifier(input_location: str, output_location: str, run_id: str) -> None:
    """
    Esta es una función de ejemplo para mostrar cómo usar Spark. 
    Simula los siguientes pasos:
        1. Limpiar datos de entrada.
        2. Usar un modelo preentrenado para hacer predicciones.
        3. Escribir predicciones en una salida HDFS.
        
    Dado que esto es un ejemplo, vamos a omitir la construcción de un modelo.
    En su lugar, marcaremos ingenuamente las reseñas que contienen el texto "good" como positivas y el resto como negativas.
    """

    # Leer entrada
    df_raw = spark.read.option("header", True).csv(input_location)

    # Limpiar texto

    # Tokenizar texto
    tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")
    df_tokens = tokenizer.transform(df_raw).select("cid", "review_token")

    # Eliminar palabras vacías
    remover = StopWordsRemover(inputCol="review_token", outputCol="review_clean")
    df_clean = remover.transform(df_tokens).select("cid", "review_clean")

    # Función para verificar la presencia de la palabra "good"
    df_out = df_clean.select(
        "cid",
        array_contains(df_clean.review_clean, "good").alias("positive_review"),
    )

    # Añadir la columna de fecha de inserción
    df_final = df_out.withColumn("insert_date", lit(run_id))

    # Escribir resultados en formato Parquet
    df_final.write.mode("overwrite").parquet(output_location)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        help="Entrada HDFS",
        default="s3a://user-analytics/raw/movie_review.csv",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Salida HDFS",
        default="s3a://user-analytics/clean/movie_review",
    )
    parser.add_argument("--run-id", type=str, help="ID de ejecución", default="2024-05-05")
    args = parser.parse_args()

    # Crear sesión de Spark
    spark = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.7.3",
        )
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    # Ejecutar la función de clasificación de texto
    random_text_classifier(
        input_location=args.input, output_location=args.output, run_id=args.run_id
    )