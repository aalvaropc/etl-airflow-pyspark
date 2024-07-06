# Usar la imagen base de Apache Airflow versión 2.9.2
FROM apache/airflow:2.9.2

# Copiar el archivo requirements.txt al directorio raíz del contenedor
COPY requirements.txt /

# Instalar las dependencias de Python especificadas en requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copiar el script quarto.sh al directorio raíz del contenedor
COPY quarto.sh /

# Ejecutar el script quarto.sh para instalar Quarto
RUN cd / && bash /quarto.sh

# Copiar el script setup_conn.py al directorio $AIRFLOW_HOME (directorio de Airflow)
COPY setup_conn.py $AIRFLOW_HOME

# Cambiar el usuario a root para instalar paquetes y configurar el entorno
USER root

# Actualizar la lista de paquetes y instalar el JDK predeterminado (Java Development Kit)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk

# Descargar Apache Spark versión 3.5.1 con Hadoop 3
RUN curl https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o spark-3.5.1-bin-hadoop3.tgz

# Cambiar permisos del archivo descargado para permitir la extracción
RUN chmod 755 spark-3.5.1-bin-hadoop3.tgz

# Crear el directorio /opt/spark y extraer el contenido del archivo tar allí, eliminando el primer nivel de directorios
RUN mkdir -p /opt/spark && tar xvzf spark-3.5.1-bin-hadoop3.tgz --directory /opt/spark --strip-components=1

# Establecer la variable de entorno JAVA_HOME al directorio del JDK
ENV JAVA_HOME='/usr/lib/jvm/java-17-openjdk-amd64'

# Agregar el binario del JDK al PATH del sistema
ENV PATH=$PATH:$JAVA_HOME/bin

# Establecer la variable de entorno SPARK_HOME al directorio de Spark
ENV SPARK_HOME='/opt/spark'

# Agregar los binarios de Spark al PATH del sistema
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
