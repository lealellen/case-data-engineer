FROM python:3.10-slim
WORKDIR /app

COPY producer.py .

RUN pip install kafka-python Faker

CMD ["python", "producer.py"]
