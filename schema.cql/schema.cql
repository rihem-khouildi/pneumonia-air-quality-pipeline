CREATE KEYSPACE IF NOT EXISTS iot_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS iot_keyspace.iot_data (
    patient_id TEXT PRIMARY KEY,
    image_path TEXT,
    label TEXT,
    heart_rate DOUBLE,
    oxygen_saturation DOUBLE,
    body_temperature DOUBLE,
    respiratory_rate DOUBLE,
    cough INT,
    chest_pain INT,
    shortness_of_breath INT,
    who_region TEXT,
    country TEXT,
    city TEXT,
    year INT,
    pm25 DOUBLE,
    pm10 DOUBLE,
    no2 DOUBLE
);
CREATE TABLE IF NOT EXISTS iot_keyspace.city_patient_summary (
    city TEXT PRIMARY KEY,
    country TEXT,
    who_region TEXT,
    year INT,
    total_patients INT,
    pneumonia_count INT,
    avg_spo2 DOUBLE,
    avg_pm25 DOUBLE,
    avg_pm10 DOUBLE,
    avg_no2 DOUBLE,
    city_risk_score DOUBLE,
    last_update TIMESTAMP
);