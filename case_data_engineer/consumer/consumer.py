from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, current_timestamp
from pyspark.sql.types import StructType, StringType, FloatType

import sys
import traceback

try:
    spark = SparkSession.builder \
        .appName("IoT Consumer") \
        .config("spark.jars", "/app/postgresql-42.7.3.jar") \
        .getOrCreate()

    # Define o schema dos dados que estão vindo da producer
    schema = StructType() \
        .add("sensor_id", StringType()) \
        .add("temperature", FloatType()) \
        .add("humidity", FloatType()) \
        .add("location", StringType()) \
        .add("timestamp", StringType()) \
        .add("status", StringType())

    # Le dados do Kafka
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "iot-sensors")
        .option("startingOffsets", "earliest")
        .load()
    )

    df_json = df_raw.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")

    # Salva bronze (dados brutos)
    df_json.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints/bronze") \
        .option("path", "/app/data/bronze") \
        .outputMode("append") \
        .start()

    # Adiciona coluna de validação para garantir integridade dos dados
    df_validated = df_json.withColumn(
        "validacao",
        when(
            col("sensor_id").isNotNull() &
            col("temperature").isNotNull() &
            col("humidity").isNotNull() &
            col("location").isNotNull() &
            col("timestamp").isNotNull() &
            (col("status").isin("OK", "WARNING", "ERROR")),
            1
        ).otherwise(0)
    )

    # Filtra os dados válidos 
    df_valid = df_validated.filter(col("validacao") == 1) \
    .withColumn("ingestion_time", current_timestamp())

    def save_to_db(batch_df, batch_id):
        """
        Função para salvar cada lote de dados válidos no banco de dados.
        """
        if batch_df.count() > 0:
            print(f"Inserindo {batch_df.count()} registros no banco (batch {batch_id})")
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/iotdb") \
                .option("dbtable", "silver_data") \
                .option("user", "postgres") \
                .option("password", "1234") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        else:
            print(f"Nenhum dado válido no batch {batch_id}")
    
    silver = df_valid.writeStream \
        .foreachBatch(save_to_db) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/silver") \
        .start()
    
    # Filtra os dados inválidos e salva em parquet 
    df_invalid = df_validated.filter(col("validacao") == 0)

    df_invalid.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints/logs") \
        .option("path", "/app/data/logs") \
        .outputMode("append") \
        .start()

    spark.streams.awaitAnyTermination()

except Exception as e:
    print("Erro ao executar o pipeline:", file=sys.stderr)
    print(traceback.format_exc(), file=sys.stderr)
    sys.exit(1)