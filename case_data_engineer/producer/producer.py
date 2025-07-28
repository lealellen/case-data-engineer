"""
Producer que simula dados de sensores IoT para Kafka.

- Cada registro possui: sensor_id, temperatura, umidade, localização, timestamp e status (OK/WARNING/ERROR).
- Antes de enviar, espera o Kafka estar disponível.
- Envia os dados em JSON para o tópico 'iot-sensors'.
"""

import time
import json
import random
from faker import Faker
from kafka import KafkaProducer
import time
import socket

def wait_for_kafka(host, port):
    """
    Aguarda o serviço Kafka estar disponível na porta especificada.
    """
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                print("Kafka disponível!!! Seguindo com o producer.")
                return
        except OSError:
            print("Aguardando Kafka subir...")
            time.sleep(3)

wait_for_kafka("kafka", 9092)


faker = Faker()
SENSOR_STATUS = ["OK", "WARNING", "ERROR"]

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data():
    """
    Gera um dicionário que simula a leitura de um sensor, 
    as vezes deixando alguns campos nulos.
    """
    data = {
        "sensor_id": faker.uuid4(),
        "temperature": round(random.uniform(15, 35), 2),
        "humidity": round(random.uniform(20, 80), 2),
        "location": faker.city(),
        "timestamp": faker.iso8601(),
        "status": random.choice(SENSOR_STATUS)
    }

    if random.random() < 0.2:
        campo_corrompido = random.choice(list(data.keys()))
        data[campo_corrompido] = None

    return data


if __name__ == "__main__":
    while True:
        data = generate_sensor_data()
        producer.send('iot-sensors', value=data)
        print(f"Sent: {data}")
        time.sleep(5)
        

