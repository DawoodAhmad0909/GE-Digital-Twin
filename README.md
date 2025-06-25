# GE-Digital-Twin
## Overview 
The GE_Digital_db database is a comprehensive smart farming data model designed to monitor and manage agricultural operations using IoT-enabled devices. It integrates device metadata, environmental sensor readings, and alerts into a unified structure to support precision agriculture and real-time decision-making.
## Objectives 
To transform raw sensor data into actionable agricultural insights for sustainable farming.
## Database Creation
``` sql
CREATE DATABASE GE_Digital_db;
USE GE_Digital_db;
```
## Table Creation
### Table:farm_devices
``` sql
CREATE TABLE farm_devices(
    device_id      VARCHAR(25) PRIMARY KEY,
    device_type    VARCHAR(50),
    location       TEXT,
    crop_type      VARCHAR(50),
    install_date   DATE,
    battery_status VARCHAR(25)
);

SELECT * FROM farm_devices;
```
### Table:farm_sensor_data
``` sql
CREATE TABLE farm_sensor_data(
    reading_id     INT PRIMARY KEY,
    device_id      VARCHAR(25),
    timestamp      DATETIME,
    soil_moisture  DECIMAL(7,2),
    air_temp       DECIMAL(7,2),
    humidity       DECIMAL(7,2),
    rainfall_mm    DECIMAL(7,2),
    wind_speed_kmh DECIMAL(7,2),
    FOREIGN KEY (device_id) REFERENCES farm_devices(device_id)
);

SELECT * FROM farm_sensor_data;
```
### Table:farm_alerts
``` sql
CREATE TABLE farm_alerts(
    alert_id   INT PRIMARY KEY,
    device_id  VARCHAR(25),
    timestamp  DATETIME,
    alert_type TEXT NOT NULL,
    severity   VARCHAR(25) NOT NULL,
    resolved   BOOLEAN,
    FOREIGN KEY (device_id) REFERENCES farm_devices(device_id)
);

SELECT * FROM farm_alerts;
```
## Key Queries

#### 1. List all devices with battery status below 50%, sorted by installation date.
``` sql
SELECT * FROM farm_devices
WHERE CAST(REGEXP_SUBSTR(battery_status,'[0-9]+') AS UNSIGNED)<50.0
ORDER BY install_date;
```
#### 2. Show devices deployed in 'Wheat' fields and their most recent activity timestamp.
``` sql
SELECT d.device_id,d.device_type,d.location,MAX(sd.timestamp) AS Last_activity
FROM farm_sensor_data sd
JOIN farm_devices d
ON d.device_id=sd.device_id
WHERE LOWER(d.crop_type)='wheat'
GROUP BY  d.device_id,d.device_type,d.location;
```
#### 3. Calculate the average days since installation for each device type
``` sql
SELECT 
        device_type,
        ROUND(AVG(TIMESTAMPDIFF(DAY,install_date,NOW())),2) AS Average_time_in_days
FROM farm_devices
GROUP BY device_type;
```
#### 4. Find soil moisture readings below 30% that triggered irrigation alerts.
``` sql
SELECT 
        sd.*,a.alert_type
FROM farm_sensor_data sd
JOIN farm_devices d
ON d.device_id=sd.device_id
JOIN farm_alerts a 
ON d.device_id=a.device_id
WHERE 
        sd.soil_moisture<30
        AND (LOWER(a.alert_type) LIKE '%irrigation%'
                OR LOWER(a.alert_type) LIKE '%soil moisture%');
```
#### 5. Display hourly temperature fluctuations exceeding 10°C within a single day. 
``` sql
SELECT *
FROM (
        SELECT
                DATE(timestamp) AS Date,
        MIN(air_temp) AS Minimum_temperature,
                MAX(air_temp) AS Maximum_temperature,
                ABS(MIN(air_temp)-MAX(air_temp)) AS Temp_difference
        FROM farm_sensor_data
        WHERE air_temp IS NOT NULL
    GROUP BY Date 
    ) AS temp_changes
HAVING Temp_difference >10;
```
#### 6. Identify periods with consecutive rainfall measurements >5mm.
``` sql
SELECT *
FROM (
        SELECT 
                device_id,timestamp,rainfall_mm,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp)-
        ROW_NUMBER() OVER (PARTITION BY device_id,CASE WHEN rainfall_mm>5 THEN 1 ELSE 0 END ORDER BY timestamp) AS rain_period_id
        FROM farm_sensor_data
    WHERE rainfall_mm IS NOT NULL
    ) AS ground_rain
WHERE rainfall_mm>5
ORDER BY device_id,timestamp;
```
#### 7. List all unresolved high/critical severity alerts with device locations.
``` sql
SELECT 
        a.* , d.location 
FROM farm_alerts a
JOIN farm_devices d 
ON a.device_id=d.device_id
WHERE 
        resolved=False 
    AND LOWER(severity) IN ('high','critical');
```
#### 8. Find devices that have triggered multiple alert types.
``` sql
SELECT 
        d.*,COUNT(DISTINCT a.alert_type) AS Total_alert_type
FROM farm_devices d
JOIN farm_alerts a 
ON a.device_id=d.device_id
GROUP BY d.device_id
HAVING Total_alert_type>1;
```
#### 9. Calculate the average time between alert generation and resolution.
``` sql
WITH alert_resolution_time AS (
    SELECT 
        a.alert_id,
        a.device_id,
        a.alert_type,
        a.timestamp AS alert_time,
        MIN(sd.timestamp) AS resolution_time
    FROM 
        farm_alerts a
    JOIN 
        farm_sensor_data sd 
        ON a.device_id = sd.device_id 
           AND sd.timestamp > a.timestamp
    WHERE 
        a.resolved = TRUE
    GROUP BY 
        a.alert_id, a.device_id, a.timestamp
),
time_differences AS (
    SELECT 
        alert_id,alert_type,
        TIMESTAMPDIFF(HOUR, alert_time, resolution_time) AS hours_to_resolve
    FROM 
        alert_resolution_time
)
SELECT 
    alert_type,ROUND(AVG(hours_to_resolve), 2) AS avg_hours_to_resolve
FROM 
    time_differences
    GROUP BY alert_type;

#### 10. Correlate high wind speed events (>15 km/h) with rainfall measurements.
 sql
SELECT 
        reading_id,device_id,timestamp,wind_speed_kmh,rainfall_mm
FROM farm_sensor_data
WHERE wind_speed_kmh>15;
```
#### 11. Show days where both low soil moisture and high temperatures occurred.
``` sql
SELECT 
        DATE(timestamp) AS Date,MIN(soil_moisture) AS Low_moisture,MAX(air_temp) AS Maximum_temperature
FROM farm_sensor_data
GROUP BY Date
HAVING MIN(soil_moisture)<35 AND MAX(air_temp)>25;
```
#### 12. Find humidity levels during irrigation valve activations.
``` sql
SELECT d.device_id,d.device_type,sd.timestamp,sd.humidity
FROM farm_devices d 
JOIN farm_sensor_data sd 
ON sd.device_id=d.device_id
WHERE 
        LOWER(d.device_type)='irrigation valve' ;
```
#### 13. Calculate average soil moisture by crop type.
``` sql
SELECT 
        d.crop_type,ROUND(AVG(sd.soil_moisture),2) AS Average_moisture
FROM farm_devices d 
JOIN farm_sensor_data sd 
ON sd.device_id=d.device_id
GROUP BY d.crop_type;
```
#### 14. Identify devices that haven't transmitted data in over 24 hours.
``` sql
SELECT 
        d.*,MAX(sd.timestamp) AS Last_contact,TIMESTAMPDIFF(HOUR,MAX(sd.timestamp),(SELECT MAX(timestamp) FROM farm_sensor_data)) AS Duration
FROM farm_devices d 
LEFT JOIN farm_sensor_data sd 
ON sd.device_id=d.device_id
GROUP BY d.device_id
HAVING Duration>24
        OR Duration IS NULL;
```
#### 15. Show the busiest data collection hour across all devices.
``` sql
SELECT HOUR(timestamp)AS Hour,COUNT(*) AS Total_data FROM farm_sensor_data
GROUP BY hour
ORDER BY Total_data DESC 
LIMIT 2;
```
#### 17. Predict irrigation needs by analyzing soil moisture depletion rates.
``` sql
SELECT *
FROM (
        SELECT 
                d.device_id,d.device_type,d.location,
                sd.soil_moisture,LAG(sd.soil_moisture) OVER (PARTITION BY device_id ORDER BY sd.timestamp) AS Previous_moisture,
                sd.timestamp,LAG(sd.timestamp) OVER (PARTITION BY device_id ORDER BY sd.timestamp) AS Previous_time,
                TIMESTAMPDIFF(HOUR,        LAG(sd.timestamp) OVER (PARTITION BY device_id ORDER BY sd.timestamp),sd.timestamp) AS Duration_HOURS,
                ROUND(        (LAG(sd.soil_moisture) OVER (PARTITION BY device_id ORDER BY sd.timestamp)-sd.soil_moisture)/
                NULLIF(TIMESTAMPDIFF(HOUR,        LAG(sd.timestamp) OVER (PARTITION BY device_id ORDER BY sd.timestamp),sd.timestamp),0),2) AS Depletion_rate
        FROM farm_devices d 
        JOIN farm_sensor_data sd 
        ON sd.device_id=d.device_id
    WHERE sd.soil_moisture IS NOT NULL
    ) AS Irrigation_needs
WHERE Depletion_rate<0;
```
#### 18. Compare morning vs. afternoon environmental conditions.
``` sql
SELECT 
        time_period,
        ROUND(AVG(soil_moisture),2) AS Average_moisture,
    ROUND(AVG(air_temp),2) AS Average_air_temp,
    ROUND(AVG(humidity),2) AS Average_humidity,
    ROUND(AVG(rainfall_mm),2) AS Average_rainfall_mm,
    ROUND(AVG(wind_speed_kmh),2) AS Average_wind_speed_kmh
FROM (
        SELECT *,
                CASE 
                        WHEN TIME(timestamp) BETWEEN '06:00:00' AND '11:59:59' THEN 'Morning'
            WHEN TIME(timestamp) BETWEEN '12:00:00' AND '17:59:59' THEN 'Afternoon'
            WHEN TIME(timestamp) BETWEEN '18:00:00' AND '05:59:59' THEN 'Night'
                END AS time_period
        FROM farm_sensor_data
) AS period_data
WHERE time_period IS NOT NULL
GROUP BY time_period
HAVING time_period IN ('Morning','Afternoon');
```
#### 19. Find devices with inconsistent data (e.g., rainfall reported without humidity).
``` sql
SELECT  d.device_id,d.device_type,d.location,sd.timestamp,sd.rainfall_mm,sd.humidity
FROM farm_devices d 
JOIN farm_sensor_data sd 
ON sd.device_id=d.device_id
WHERE sd.rainfall_mm IS NOT NULL AND sd.humidity IS NULL;
```
#### 20. Identify sensors needing calibration (values stuck at constants for 12+ hours).
``` sql
SELECT 
    device_id,
    'soil_moisture' AS sensor_type,
    soil_moisture AS sensor_value,
    MIN(timestamp) AS start_time,
    MAX(timestamp) AS end_time,
    TIMESTAMPDIFF(HOUR, MIN(timestamp), MAX(timestamp)) AS duration_hours,
    COUNT(*) AS reading_count
FROM farm_sensor_data
WHERE soil_moisture IS NOT NULL
GROUP BY device_id, soil_moisture
HAVING duration_hours >= 12 AND COUNT(*) >= 2

UNION

SELECT 
    device_id,
    'air_temp' AS sensor_type,
    air_temp AS sensor_value,
    MIN(timestamp),
    MAX(timestamp),
    TIMESTAMPDIFF(HOUR, MIN(timestamp), MAX(timestamp)) AS duration_hours,
    COUNT(*)
FROM farm_sensor_data
WHERE air_temp IS NOT NULL
GROUP BY device_id, air_temp
HAVING duration_hours >= 12 AND COUNT(*) >= 2

UNION

SELECT 
    device_id,
    'humidity' AS sensor_type,
    humidity AS sensor_value,
    MIN(timestamp),
    MAX(timestamp),
    TIMESTAMPDIFF(HOUR, MIN(timestamp), MAX(timestamp)) AS duration_hours,
    COUNT(*)
FROM farm_sensor_data
WHERE humidity IS NOT NULL
GROUP BY device_id, humidity
HAVING duration_hours >= 12 AND COUNT(*) >= 2

UNION

SELECT 
    device_id,
    'rainfall_mm' AS sensor_type,
    rainfall_mm AS sensor_value,
    MIN(timestamp),
    MAX(timestamp),
    TIMESTAMPDIFF(HOUR, MIN(timestamp), MAX(timestamp))AS duration_hours,
    COUNT(*)
FROM farm_sensor_data
WHERE rainfall_mm IS NOT NULL
GROUP BY device_id, rainfall_mm
HAVING duration_hours >= 12 AND COUNT(*) >= 2

UNION

SELECT 
    device_id,
    'wind_speed_kmh' AS sensor_type,
    wind_speed_kmh AS sensor_value,
    MIN(timestamp),
    MAX(timestamp),
    TIMESTAMPDIFF(HOUR, MIN(timestamp), MAX(timestamp)) AS duration_hours,
    COUNT(*)
FROM farm_sensor_data
WHERE wind_speed_kmh IS NOT NULL
GROUP BY device_id, wind_speed_kmh
HAVING duration_hours >= 12 AND COUNT(*) >= 2;
```
## Conclusion:
The GE_Digital_db smart farming database is a robust, IoT-integrated system that enhances agricultural productivity by:

#### •Tracking real-time environmental metrics across various field devices.

#### •Generating actionable insights for crop health, irrigation, and risk alerts.

#### •Supporting sustainable agriculture by enabling data-driven decisions for resource optimization.


