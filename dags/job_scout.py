import sys
import os
import json
from kafka import KafkaProducer

# Pfad zum Scripts Ordner hinzufügen,wrapper wird importiert
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from scripts.adzuna_pipeline import fetch_adzuna_jobs, transform_jobs

def start_scout():
    """
    Nutzt Wrapper und sendet Daten an kafka
    """
    #Konfiguration aus Umgebungsvariablen
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka-broker:9094")
    
    # Kafka Producer Setup
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=5
    )
    
    print("Starte Datenerfassung...")
    
    # Daten über Wrapper abrufen
    raw_jobs = fetch_adzuna_jobs()
    
    if not raw_jobs:
        print("Keine neuen Jobs von der API erhalten.")
        producer.close()
        return

    #Daten transformieren
    clean_jobs = transform_jobs(raw_jobs)
    
    print(f"{len(clean_jobs)} Jobs aufbereitet. Sende an Kafka-Topic 'it-jobs-raw'...")
    
    # 3. Schritt: Senden an Kafka
    for job in clean_jobs:
        # .get(timeout=10) zwingt das Skript zu warten, bis Kafka "OK" sagt
        future = producer.send('it-jobs-raw', value=job)
        try:
            record_metadata = future.get(timeout=10)
            print(f"Erfolgreich gesendet: {job['title']} (Partition: {record_metadata.partition})")
        except Exception as e:
            print(f"Fehler beim Senden von Job {job.get('title')}: {e}")
    
    # Sicherstellen, dass alles übertragen wurde
    producer.flush()
    producer.close()
    print("\nBatch erfolgreich abgeschlossen.")

if __name__ == "__main__":
    start_scout()
