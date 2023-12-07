CREATE TABLE table_m3 (
    "Name" VARCHAR(255),
    "Brand" VARCHAR(255),
    "Model" VARCHAR(255),
    "Battery capacity (mAh)" INT,
    "Screen size (inches)" FLOAT,
    "Touchscreen" VARCHAR(5), 
    "Resolution x" INT,
    "Resolution y" INT,
    "Processor" INT,
    "RAM (MB)" INT,
    "Internal storage (GB)" FLOAT,
    "Rear camera" FLOAT,
    "Front camera" FLOAT,
    "Operating system" VARCHAR(255),
    "Wi_Fi" VARCHAR(255),
    "Bluetooth" VARCHAR(255),
    "GPS" VARCHAR(255),
    "Number of SIMs" INT,
    "3G" VARCHAR(5),  
    "4G/ LTE" VARCHAR(5),
    "Price" INT
);

-- Menginput data dari file '.csv' kedalam tabel 'table_m3'
COPY table_m3
FROM '/files/P2M3_furqon_data_raw.csv'
DELIMITER ','
CSV HEADER;