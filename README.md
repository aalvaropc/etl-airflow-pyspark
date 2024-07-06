# Análisis de Comportamiento de Usuario

## Descripción del Proyecto

Este proyecto tiene como objetivo construir una tubería de datos automatizada para poblar la tabla `user_behavior_metric`. Esta tabla OLAP es utilizada por analistas y software de tablero para generar métricas de comportamiento de usuario a partir de datos de compras y reseñas de películas. 

El proyecto incluye las siguientes tecnologías:
- **Airflow:** Para programar y orquestar DAGs.
- **Postgres:** Para almacenar detalles de Airflow y datos de compras de usuarios.
- **DuckDB:** Como almacén de datos para procesamiento.
- **Quarto con Plotly:** Para generar visualizaciones en HTML.
- **Apache Spark:** Para procesar datos y ejecutar algoritmos de clasificación.
- **Minio:** Como sistema de almacenamiento compatible con S3.


## Arquitectura
<img src="https://github.com/aalvaropc/etl-airflow-pyspark/blob/main/images/diagrama.png" alt="Texto alternativo" width="800" height="400">

## Panel de Control de Airflow
<img src="https://github.com/aalvaropc/etl-airflow-pyspark/blob/main/images/airflow.png" alt="Texto alternativo" width="800" height="500">

