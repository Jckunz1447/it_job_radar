import json
import os
import psycopg2
from kafka import KafkaConsumer, TopicPartition
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from datetime import datetime 
# .env laden 1 Verezichnis weiter oben
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def start_consumer():
    #Konfig
    bootstrap = "localhost:9092"
    topic = 'it-jobs-raw'
    
    # 1.datenbank verbindung postgres
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("POSTGRES_DB", "airflow"),
            user=os.getenv("POSTGRES_USER", "airflow_user"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("DB_PORT", "5432")
        )
        cur = conn.cursor()
        print("Erfolgreich mit PostgreSQL verbunden.")
     #falls db noch nicht exisitiert wird sie erstellt
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                source VARCHAR(50),
                external_id VARCHAR(100) UNIQUE,
                title VARCHAR(255),
                company VARCHAR(255),
                location VARCHAR(255),
                created TIMESTAMP,
                salary_min NUMERIC,
                redirect_url TEXT
            );
        """)
        conn.commit()
        print("Datenbank-Tabelle 'jobs' ist bereit.")
    except Exception as e:
        print(f"PostgreSQL Verbindungsfehler: {e}")
        return

    # 2. ELASTICSEARCH-VERBINDUNG
    try:
        #innerhalb dockers
        es = Elasticsearch(["http://localhost:9200"])
        if es.ping():
            print(" Erfolgreich mit Elasticsearch verbunden.")
        else:
            print("Elasticsearch erreichbar, aber Ping fehlgeschlagen.")
    except Exception as e:
        print(f"Elasticsearch Verbindungsfehler: {e}")
        es = None

    # 3.kafka consumer setup
    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=10000 
    )
    
    # Manuelle Zuweisung für stabilere Verbindung im Docker-Umfeld
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning()
    
    print(f" Consumer aktiv. Verarbeite Nachrichten von {topic}...")

    try:    
        while True:
            messages = consumer.poll(timeout_ms=1000)
            
            for tp, msgs in messages.items():
                for msg in msgs:
                    job = msg.value
                    print(f"🔍 Verarbeite Job: {job.get('title')} @ {job.get('company')}")
                    
                    #in postgres sql speichern
                    try:
                        insert_query = """
                        INSERT INTO jobs (
                            source, external_id, title, company, 
                            location, created, salary_min, redirect_url
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (external_id) DO NOTHING;
                        """
                        cur.execute(insert_query, (
                            job.get('source'),
                            job.get('external_id'),
                            job.get('title'),
                            job.get('company'),
                            job.get('location'),
                            job.get('created'),
                            job.get('salary_min'),
                            job.get('redirect_url')
                        ))
                        conn.commit()
                        print(f" In Postgres gespeichert.")
                    except Exception as pg_e:
                        print(f"Postgres Fehler: {pg_e}")
                        conn.rollback()

                    # indizieren in elasticsearch
                    if es:
                        try:
                            es_doc = {
                                "source": job.get('source'),
                                "external_id": job.get('external_id'),
                                "title": job.get('title'),
                                "company": job.get('company'),
                                "location": job.get('location'),
                                "created": job.get('created'),
                                "salary_min": job.get('salary_min'),
                                "redirect_url": job.get('redirect_url'),
                                "ingested_at": datetime.now().isoformat() # Zeitstempel für Kibana
                            }
                            es.index(index="it-jobs-index", id=job.get('external_id'), document=es_doc)
                            print(f" In Elasticsearch indiziert.")
                        except Exception as es_e:
                            print(f"Elasticsearch Fehler: {es_e}")
            
    except KeyboardInterrupt:
        print("\n Consumer manuell gestoppt.")
    except Exception as e:
        print(f" Kritischer Fehler: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()
        consumer.close()
        print(" Verbindungen sauber geschlossen.")

if __name__ == "__main__":
    start_consumer()
