from kafka import KafkaProducer
import pandas as pd
import json
import time

# Veri dosyasını yükle
data_address = "cleaningdataset.csv"
data = pd.read_csv(data_address)

# Kafka Producer oluştur
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "data-kafka"

# Verileri Kafka'ya gönder
for index, row in data.iterrows():
    message = row.to_dict()
    producer.send(topic, message)
    print(f"Sent: {message} + xyzt")
    time.sleep(0.1)  # Veri akışını simüle etmek için bekleme

producer.close()
