import requests
import json
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

# lädt variablen aus der .env Datei
load_dotenv()

# Konfiguration (aus der .env geladen)
APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

QUERY = "Python"
LOCATION = "Berlin"

def fetch_adzuna_jobs():
    print(f"Suche nach {QUERY} Jobs in {LOCATION}...")
    # Adzuna ApI url für Deutschland
    params = {
        'app_id': APP_ID,
        'app_key': APP_KEY,
        'results_per_page': 5,
        'what': QUERY,
        #'where': LOCATION,
        'content-type': 'application/json' 
    }
    url = "https://api.adzuna.com/v1/api/jobs/de/search/1"

    try:
        response = requests.get(url, params=params)
        print(f"API Status: {response.status_code}")
      
        if response.status_code != 200:
            print(f"Fehler-Details: {response.text}")
            return [] 
    
        data = response.json()
        results = data.get('results', [])
        print(f"Gefundene Jobs: {len(results)}")
        return results
           
    except Exception as e:
        print(f" Fehler beim API-Abruf: {e}")
        return []

def start_scout():
    # Muss sowohl lokal auf Rechner laufen localhost als auch innerhalb Dockers kafka
    bootstrap = KAFKA_BOOTSTRAP

    # Kafka Producer setup
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    jobs = fetch_adzuna_jobs()
   
    if not jobs:
        print("Keine Jobs gefunden oder Api ist fehlgeschlagen.")
        return
    
    print(f"{len(jobs)} Jobs gefunden. Sende an Kafka...")
   
    for job in jobs:
        # Daten aufbereiten
        job_data = {
            "source": "adzuna",
            "external_id": job.get('id'),
            "title": job.get('title'),
            "company": job.get('company', {}).get('display_name'),
            "location": job.get('location', {}).get('display_name'),
            "created": job.get('created'),
            "salary_min": job.get('salary_min'),
            "redirect_url": job.get("redirect_url")
        }
        # Python Dictionary wird von value_serializer in binären Json String umgewandelt
        producer.send('it-jobs-raw', value=job_data)
        print(f"Gesendet: {job_data['title']} bei {job_data['company']}")
        
    # Nachrichten im Zwischenspeicher werden an Kafka Broker gesendet
    producer.flush()
    producer.close()
    print("\n Alle Jobs wurden erfolreich an Kafka übertragen")

if __name__ == "__main__":
    start_scout()
