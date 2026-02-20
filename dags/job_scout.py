import requests
import json
import time
from kafka import KafkaProducer


# lädt variablen aus der .env Datei
load_dotenv()

#Konfiguration

APP_ID = "239209ff"
APP_KEY = "9023b3224c109b13c6119a36f8aced07"
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
        results =  data.get('results', [])
        print(f"Gefundene Jobs: {len(results)}")
        return results
           
    except Exception as e:
        print(f" Fehler beim API-Abruf: {e}")
        return []
def start_scout():
#Muss sowohl lokal auf Rechner laufen localhost als auch innerhalb Dockers kafka
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

#Kafka Producer setup
    producer = KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    jobs = fetch_adzuna_jobs()
   
    if not jobs:
        print("Keine Jobs gefunden oder Api ist fehlgeschlagen.")
        return
    
    print(f"{len(jobs)} Jobs gefunden. Sende an Kafka...")
   
    for job in jobs:
        #Daten aufbereiten
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
        #Python Dictionary wird von value_serializer in  binären Json String umgewandelt
        producer.send('it-jobs-raw', value=job_data)
        print(f"Gesendet: {job_data['title']} bei {job_data['company']}")
         
        #Nachrichten im Zwischenspeicher werden an Kafka Broker gesendet
        producer.flush()
        producer.close()
        print("\n Alle Jobs wurden erfolreich an Kafka übertragen")

if __name__ == "__main__":
    start_scout()
