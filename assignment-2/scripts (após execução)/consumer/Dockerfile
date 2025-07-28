FROM bitnami/spark:latest

ENV SPARK_EXTRA_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0

USER root
WORKDIR /app

COPY consumer.py .
COPY postgresql-42.7.3.jar . 

CMD ["/opt/bitnami/spark/bin/spark-submit", "--jars", "/app/postgresql-42.7.3.jar", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0", "/app/consumer.py"]
