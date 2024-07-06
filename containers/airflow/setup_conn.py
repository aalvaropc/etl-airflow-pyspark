import subprocess

# Definir detalles de la conexión
conn_id = "aws_default"
conn_type = "aws"
access_key = "minio"
secret_key = "minio123"
region_name = "us-east-1"
endpoint_url = "http://minio:9000"

# Construir el JSON adicional
extra = {
    "aws_access_key_id": access_key,
    "aws_secret_access_key": secret_key,
    "region_name": region_name,
    "host": endpoint_url,
}

# Convertir a cadena JSON
extra_json = str(extra).replace("'", '"')

# Definir el comando CLI
command = [
    "airflow",
    "connections",
    "add",
    conn_id,
    "--conn-type",
    conn_type,
    "--conn-extra",
    extra_json,
]
# Ejecutar el comando
subprocess.run(command)

def add_airflow_connection() -> None:
    """
    Agrega una nueva conexión en Airflow para Spark.

    Esta función agrega una nueva conexión a Airflow con el ID de conexión, tipo de conexión,
    host y puerto especificados. Utiliza el comando `airflow connections add` para agregar la conexión.

    Returns:
        None

    Raises:
        None
    """
    connection_id = "spark-conn"
    connection_type = "spark"
    host = "spark://192.168.0.1"
    port = "7077"
    cmd = [
        "airflow",
        "connections",
        "add",
        connection_id,
        "--conn-host",
        host,
        "--conn-type",
        connection_type,
        "--conn-port",
        port,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Conexión {connection_id} agregada exitosamente")
    else:
        print(f"Fallo al agregar la conexión {connection_id}: {result.stderr}")


add_airflow_connection()