--erstellt tabelle für Jobs, falls noch nicht existiert
CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50),
    external_id VARCHAR(100) UNIQUE,
    title VARCHAR(255),
    company VARCHAR(255),
    location VARCHAR(255),
    created TIMESTAMP,
    salary_min NUMERIC,
    redirect_url TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--index für schnelle Suche nach Id
CREATE INDEX IF NOT EXISTS idx_external_id ON jobs(external_id);
