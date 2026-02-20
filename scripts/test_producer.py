import json
import time
import sys
from kafka import KafkaProducer

print("Starte Producer...")

try:
    # Wir nutzen 127.0.0.1 statt localhost
    producer = KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',        # Warte auf Bestätigung vom Broker
        retries=5          # Versuche es bei Fehlern 5-mal
    )
    
    job = {
        "id": int(time.time()), # Dynamische ID
        "title": "Data Engineer",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    print(f"Sende Daten an Kafka: {job}")
    
    # Wir fangen das Ergebnis des Sendens ab
    future = producer.send('it-jobs-raw', value=job)
    
    print("Warte auf Bestätigung vom Server...")
    record_metadata = future.get(timeout=10) # Erzwingt das Warten auf Antwort
    
    print(f"✅ ERFOLG! Gesendet an Topic: {record_metadata.topic}")
    print(f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

except Exception as e:
    print(f"❌ FEHLER: {e}")
    sys.exit(1)
finally:
    if 'producer' in locals():
        producer.close()
