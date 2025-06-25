CREATE DATABASE GE_Digital_db;
USE GE_Digital_db;

CREATE TABLE farm_devices(
    device_id      VARCHAR(25) PRIMARY KEY,
    device_type    VARCHAR(50),
    location       TEXT,
    crop_type      VARCHAR(50),
    install_date   DATE,
    battery_status VARCHAR(25)
);

SELECT * FROM farm_devices;

INSERT INTO farm_devices VALUES
	('SM-AG01', 'Soil Sensor', 'Field A1', 'Wheat', '2023-03-10', '87%'),
	('SM-AG02', 'Weather Station', 'Field B3', 'Corn', '2023-04-15', '92%'),
	('SM-AG03', 'Drone', 'Storage Shed', 'Mixed', '2023-05-20', '41%'),
	('SM-AG04', 'Irrigation Valve', 'Field A2', 'Wheat', '2023-02-28', '100%'),
	('SM-AG05', 'Livestock Tracker', 'Pasture North', 'Dairy', '2023-06-05', '63%');

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

INSERT INTO farm_sensor_data VALUES
	(1, 'SM-AG01', '2023-08-01 06:00:00', 34.2, 22.1, 65.0, NULL, NULL),
	(2, 'SM-AG02', '2023-08-01 06:05:00', NULL, 21.8, 68.0, 0.0, 5.2),
	(3, 'SM-AG01', '2023-08-01 12:00:00', 28.7, 29.4, 42.0, NULL, NULL),
	(4, 'SM-AG02', '2023-08-01 12:05:00', NULL, 30.1, 38.0, 2.3, 12.8),
	(5, 'SM-AG04', '2023-08-01 18:30:00', 41.5, NULL, NULL, NULL, NULL),
	(6, 'SM-AG02', '2023-08-02 06:00:00', NULL, 19.5, 72.0, 1.5, 3.7),
	(7, 'SM-AG01', '2023-08-02 06:00:00', 37.8, 20.3, 70.0, NULL, NULL),
	(8, 'SM-AG03', '2023-08-02 10:00:00', NULL, 25.7, 55.0, NULL, 8.4),
	(9, 'SM-AG04', '2023-08-02 18:00:00', 39.2, NULL, NULL, NULL, NULL),
	(10, 'SM-AG02', '2023-08-02 18:05:00', NULL, 24.9, 58.0, 0.0, 6.1),
	(11, 'SM-AG01', '2023-08-03 06:00:00', 31.5, 18.7, 75.0, NULL, NULL),
	(12, 'SM-AG02', '2023-08-03 06:05:00', NULL, 18.2, 78.0, 4.2, 9.5),
	(13, 'SM-AG05', '2023-08-03 08:00:00', NULL, 20.5, 65.0, NULL, NULL),
	(14, 'SM-AG04', '2023-08-03 12:00:00', 45.1, NULL, NULL, NULL, NULL),
	(15, 'SM-AG02', '2023-08-03 12:05:00', NULL, 27.8, 45.0, 0.0, 15.3),
	(16, 'SM-AG01', '2023-08-04 06:00:00', 29.8, 17.6, 82.0, NULL, NULL),
	(17, 'SM-AG02', '2023-08-04 06:05:00', NULL, 17.1, 85.0, 6.8, 4.2),
	(18, 'SM-AG03', '2023-08-04 14:00:00', NULL, 23.4, 60.0, NULL, 11.7),
	(19, 'SM-AG04', '2023-08-04 18:00:00', 43.7, NULL, NULL, NULL, NULL),
	(20, 'SM-AG02', '2023-08-04 18:05:00', NULL, 21.3, 62.0, 0.0, 7.9);

CREATE 	TABLE farm_alerts(
    alert_id   INT PRIMARY KEY,
    device_id  VARCHAR(25),
    timestamp  DATETIME,
    alert_type TEXT NOT NULL,
    severity   VARCHAR(25) NOT NULL,
    resolved   BOOLEAN,
    FOREIGN KEY (device_id) REFERENCES farm_devices(device_id)
);

SELECT * FROM farm_alerts;

INSERT INTO farm_alerts VALUES
	(1, 'SM-AG01', '2023-08-01 12:00:00', 'Low Soil Moisture', 'Medium', TRUE),
	(2, 'SM-AG02', '2023-08-01 12:05:00', 'High Wind Speed', 'High', TRUE),
	(3, 'SM-AG03', '2023-08-02 10:00:00', 'Low Battery', 'Critical', FALSE),
	(4, 'SM-AG01', '2023-08-03 06:00:00', 'High Humidity', 'Low', TRUE),
	(5, 'SM-AG04', '2023-08-04 18:00:00', 'Over-Irrigation', 'High', FALSE);

SELECT * FROM farm_devices
WHERE CAST(REGEXP_SUBSTR(battery_status,'[0-9]+') AS UNSIGNED)<50.0
ORDER BY install_date;

SELECT d.device_id,d.device_type,d.location,MAX(sd.timestamp) AS Last_activity
FROM farm_sensor_data sd
JOIN farm_devices d
ON d.device_id=sd.device_id
WHERE LOWER(d.crop_type)='wheat'
GROUP BY  d.device_id,d.device_type,d.location;

SELECT 
	device_type,
	ROUND(AVG(TIMESTAMPDIFF(DAY,install_date,NOW())),2) AS Average_time_in_days
FROM farm_devices
GROUP BY device_type;

SELECT 
	sd.*,a.alert_type
FROM farm_sensor_data sd
JOIN farm_devices d
ON d.device_id=sd.device_id
JOIN farm_alerts a 
ON d.device_id=a.device_id
WHERE 
	sd.soil_moisture<30
	AND LOWER(a.alert_type) LIKE '%irrigation%';
    
SELECT *
FROM (
	SELECT
		device_id,DATE(timestamp) AS Date,timestamp,air_temp,
		LAG(air_temp) OVER ( PARTITION BY device_id , DATE(timestamp) ORDER BY timestamp) AS Prev_temperature,
		ABS(air_temp-LAG(air_temp) OVER ( PARTITION BY device_id , DATE(timestamp) ORDER BY timestamp)) AS Temp_difference
	FROM farm_sensor_data
	WHERE air_temp IS NOT NULL
    ) AS temp_changes
HAVING Temp_difference >10;

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

SELECT 
	a.* , d.location 
FROM farm_alerts a
JOIN farm_devices d 
ON a.device_id=d.device_id
WHERE 
	resolved=False 
    AND LOWER(severity) IN ('high','critical');
    
SELECT 
	d.*,COUNT(DISTINCT a.alert_type) AS Total_alert_type
FROM farm_devices d
JOIN farm_alerts a 
ON a.device_id=d.device_id
GROUP BY d.device_id
HAVING Total_alert_type>1;

WITH alert_resolution_time AS (
    SELECT 
        a.alert_id,
        a.device_id,
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
        alert_id,
        TIMESTAMPDIFF(HOUR, alert_time, resolution_time) AS hours_to_resolve
    FROM 
        alert_resolution_time
)
SELECT 
    ROUND(AVG(hours_to_resolve), 2) AS avg_hours_to_resolve
FROM 
    time_differences;
    
SELECT 
	reading_id,device_id,timestamp,wind_speed_kmh,rainfall_mm
FROM farm_sensor_data
WHERE wind_speed_kmh>15;

SELECT 
	DATE(timestamp) AS Date,device_id,soil_moisture,air_temp
FROM farm_sensor_data
WHERE soil_moisture<25 AND air_temp>45;
    
SELECT d.device_id,d.device_type,sd.timestamp,sd.humidity
FROM farm_devices d 
JOIN farm_sensor_data sd 
ON sd.device_id=d.device_id
WHERE 
	LOWER(d.device_type)='irrigation valve' ;

SELECT 
	d.crop_type,ROUND(AVG(sd.soil_moisture),2) AS Average_moisture
FROM farm_devices d 
JOIN farm_sensor_data sd 
ON sd.device_id=d.device_id
GROUP BY d.crop_type;

SELECT 
	d.*,TIMESTAMPDIFF(HOUR,MAX(sd.timestamp),(SELECT MAX(timestamp) FROM farm_sensor_data)) AS Duration
FROM farm_devices d 
LEFT JOIN farm_sensor_data sd 
ON sd.device_id=d.device_id
GROUP BY d.device_id
HAVING Duration>24
	OR Duration IS NULL;

SELECT HOUR(timestamp)AS Hour,COUNT(*) AS Total_data FROM farm_sensor_data
GROUP BY hour
ORDER BY Total_data DESC 
LIMIT 1;

SELECT 
	d.device_id,d.device_type,
    sd.soil_moisture,LAG(sd.soil_moisture) OVER (PARTITION BY device_id ORDER BY sd.timestamp) AS Previous_moisture,
	sd.timestamp,LAG(sd.timestamp) OVER (PARTITION BY device_id ORDER BY sd.timestamp) AS Previous_time,
	TIMESTAMPDIFF(HOUR,	LAG(sd.timestamp) OVER (PARTITION BY device_id ORDER BY sd.timestamp),sd.timestamp) AS Duration_HOURS,
    ROUND(	(LAG(sd.soil_moisture) OVER (PARTITION BY device_id ORDER BY sd.timestamp)-sd.soil_moisture)/
    NULLIF(TIMESTAMPDIFF(HOUR,	LAG(sd.timestamp) OVER (PARTITION BY device_id ORDER BY sd.timestamp),sd.timestamp),0),2) AS Depletion_rate
FROM farm_devices d 
JOIN farm_sensor_data sd 
ON sd.device_id=d.device_id
WHERE sd.soil_moisture IS NOT NULL ;

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
GROUP BY time_period;

SELECT  d.device_id,d.device_type,d.location,sd.timestamp,sd.rainfall_mm,sd.humidity
FROM farm_devices d 
JOIN farm_sensor_data sd 
ON sd.device_id=d.device_id
WHERE sd.rainfall_mm IS NOT NULL AND sd.humidity IS NULL;
    
SELECT 
	d.device_id,d.device_type,sd.soil_moisture,
    MIN(sd.timestamp) AS Start_time,
    MAX(sd.timestamp) AS End_time,
    TIMESTAMPDIFF(HOUR,MIN(sd.timestamp),MAX(sd.timestamp)) AS Duration,
	COUNT(sd.reading_id) AS Total_readings
FROM farm_devices d 
JOIN farm_sensor_data sd 
ON sd.device_id=d.device_id
WHERE sd.soil_moisture IS NOT NULL AND LOWER(d.device_type) LIKE '%sensor%'
GROUP BY d.device_id,d.device_type,sd.soil_moisture
HAVING Duration>=12 AND Total_readings>=2;