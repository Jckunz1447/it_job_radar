import os
import requests
from dotenv import load_dotenv

# Lädt Umgebungsvariablen aus der .env Datei
load_dotenv()

# API-Zugangsdaten für Adzuna (werden aus .env gelesen)
APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")

# Suchparameter für die API-Abfrage
QUERY = "Python"
LOCATION = "Berlin"

def fetch_adzuna_jobs():
    """
    Ruft Jobdaten von der Adzuna API ab.
    Gibt eine Liste von Roh-Jobdaten (JSON) zurück.
    """
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "results_per_page": 50,
        "what": QUERY,
        # "where": LOCATION,  # optional aktivieren, falls Standortfilter gewünscht ist
        "content-type": "application/json"
    }

    # API-Endpunkt für Deutschland (Seite 1 der Ergebnisse)
    url = "https://api.adzuna.com/v1/api/jobs/de/search/1"

    # HTTP-Request an die API senden
    response = requests.get(url, params=params)

    # Antwort als JSON parsen
    data = response.json()

    # Nur das Feld "results" (Liste von Jobs) zurückgeben
    return data.get("results", [])


def transform_jobs(jobs):
    """
    Transformiert die Rohdaten der API in ein einheitliches Datenformat.
    Nur die relevanten Felder werden extrahiert.
    """
    transformed = []

    for job in jobs:
        # Datenstruktur für einen einzelnen Job
        job_data = {
            "source": "adzuna",
            "external_id": job.get("id"),
            "title": job.get("title"),
            "company": job.get("company", {}).get("display_name"),
            "location": job.get("location", {}).get("display_name"),
            "created": job.get("created"),
            "salary_min": job.get("salary_min"),
            "redirect_url": job.get("redirect_url"),
        }

        transformed.append(job_data)

    return transformed


if __name__ == "__main__":
    """
    Hauptausführung:
    1. API-Daten abrufen
    2. Daten transformieren
    3. Beispielhafte Ausgabe zur Kontrolle
    """
    jobs = fetch_adzuna_jobs()
    jobs_clean = transform_jobs(jobs)

    # Ausgabe der ersten zwei transformierten Datensätze zur Überprüfung
    print(jobs_clean[:2])
