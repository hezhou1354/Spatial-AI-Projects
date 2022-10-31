-- create a table for locations
-- you might want to create two tables for the two location sets, same for the following code


DROP TABLE IF EXISTS sample_locations_subset1;
CREATE TABLE sample_locations_subset1(
  sensor_id INTEGER PRIMARY KEY,
  lon DOUBLE PRECISION NOT NULL,
  lat DOUBLE PRECISION NOT NULL);
  
DROP TABLE IF EXISTS sample_locations_subset2;
CREATE TABLE sample_locations_subset2(
  sensor_id INTEGER PRIMARY KEY,
  lon DOUBLE PRECISION NOT NULL,
  lat DOUBLE PRECISION NOT NULL);

  
--- Import the sampled datasets of air quality sensor locations
/*
COPY sample_locations_subset1(sensor_id, lon, lat)
FROM '/Users/hezhou/Documents/Spring2022/CSci8980-003/Homework/Hw1/ca_purple_air_locations_subset.csv'
DELIMITER ','
CSV HEADER;

COPY sample_locations_subset2(sensor_id, lon, lat)
FROM '/Users/hezhou/Documents/Spring2022/CSci8980-003/Homework/Hw1/ca_purple_air_locations_subset2.csv'
DELIMITER ','
CSV HEADER;
*/

--- UNION the two tables into one
DROP TABLE IF EXISTS sample_locations;
CREATE TABLE sample_locations AS
SELECT * FROM sample_locations_subset1
UNION
SELECT * FROM sample_locations_subset2





