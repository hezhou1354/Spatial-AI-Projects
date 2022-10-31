-- create postgis extension
CREATE EXTENSION postgis;

-- create a table for locations
-- you might want to create two tables for the two location sets, same for the following code
DROP TABLE IF EXISTS sample_locations;
CREATE TABLE sample_locations(
  sensor_id INTEGER PRIMARY KEY,
  lon DOUBLE PRECISION NOT NULL,
  lat DOUBLE PRECISION NOT NULL);
 
--- Import the sampled datasets of air quality sensor locations
/*
COPY sample_locations(sensor_id, lon, lat)
FROM '/Users/hezhou/Documents/Spring2022/CSci8980-003/Homework/Hw1/ca_purple_air_locations_subset.csv'
DELIMITER ','
CSV HEADER;

COPY sample_locations(sensor_id, lon, lat)
FROM '/Users/hezhou/Documents/Spring2022/CSci8980-003/Homework/Hw1/ca_purple_air_locations_subset2.csv'
DELIMITER ','
CSV HEADER;
*/

-- create a table for buffers
DROP TABLE IF EXISTS sample_location_buffers;
CREATE TABLE sample_location_buffers(
	gid BIGSERIAL PRIMARY KEY,
	sensor_id INTEGER,
	lon DOUBLE PRECISION NOT NULL,
	lat DOUBLE PRECISION NOT NULL,
	buffer_size INTEGER NOT NULL,
	buffer geometry(Polygon,4326) NOT NULL);
CREATE INDEX "sample_location_buffers_buffer_idx" ON sample_location_buffers USING gist(buffer);

-- insert buffers into the buffer table
--- Compute and insert the buffers of size 100, 500 and 1,000 meters to the table
--- Note: transform 'epsg:4326' geometries to a coordinate system with a meter unit
--- buffer_size = 100
INSERT INTO sample_location_buffers (sensor_id, lon, lat, buffer_size, buffer)
SELECT 
	sensor_id,
	lon,
	lat,
	100, -- radius of buffer (meters)
	ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(ST_MakePoint(sample_locations.lon, 
											     				sample_locations.lat), 4326), 3857),
						                -- transform to epsg:3857, unit in meter
			               100, -- radius of buffer (meters)
			               'quad_segs=8'), -- buffer generated
		         4326)
FROM sample_locations;
--- buffer_size = 500
INSERT INTO sample_location_buffers (sensor_id, lon, lat, buffer_size, buffer)
SELECT 
	sensor_id,
	lon,
	lat,
	500, -- radius of buffer (meters)
	ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(ST_MakePoint(sample_locations.lon, 
											     				sample_locations.lat), 4326), 3857),
						                -- transform to epsg:3857, unit in meter
			               500, -- radius of buffer (meters)
			               'quad_segs=8'), -- buffer generated
		         4326)
FROM sample_locations;
--- buffer_size = 1000
INSERT INTO sample_location_buffers (sensor_id, lon, lat, buffer_size, buffer)
SELECT 
	sensor_id,
	lon,
	lat,
	1000, -- radius of buffer (meters)
	ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(ST_MakePoint(sample_locations.lon, 
											     				sample_locations.lat), 4326), 3857),
						                -- transform to epsg:3857, unit in meter
			               1000, -- radius of buffer (meters)
			               'quad_segs=8'), -- buffer generated
		         4326)
FROM sample_locations;
----------------------------------------------------------------------------



CREATE INDEX "line_features_wkb_geometry_geom_idx" ON line_features USING gist(wkb_geometry);
CREATE INDEX "point_features_wkb_geometry_geom_idx" ON point_features USING gist(wkb_geometry);
CREATE INDEX "polygon_features_wkb_geometry_geom_idx" ON polygon_features USING gist(wkb_geometry);

-- create a table for geographic features
DROP TABLE IF EXISTS geographic_features;
CREATE TABLE geographic_features(
	gid BIGSERIAL PRIMARY KEY,
	sensor_id INTEGER NOT NULL,
	geom_type TEXT NOT NULL,
	geo_feature TEXT NOT NULL,
	feature_type TEXT NOT NULL,
	buffer_size INTEGER NOT NULL,
	value  DOUBLE PRECISION);

-- insert geographic features into the geographic_features table
--- point
DROP TABLE IF EXISTS point_geographic_features;
CREATE TABLE point_geographic_features AS
SELECT 
	b.sensor_id, 
	'point' AS geom_type, 
	p.geo_feature, 
	p.feature_type, 
	b.buffer_size, 
	COUNT(b.sensor_id) AS value
FROM 
	sample_location_buffers AS b, 
	point_features AS p
WHERE 
	ST_Intersects(b.buffer, p.wkb_geometry)
GROUP BY
	b.sensor_id, 
	p.geo_feature, 
	p.feature_type, 
	b.buffer_size;
	
--- line
DROP TABLE IF EXISTS line_geographic_features;
CREATE TABLE line_geographic_features AS
SELECT 
	b.sensor_id, 
	'line' AS geom_type, 
	l.geo_feature, 
	l.feature_type, 
	b.buffer_size, 
	SUM(ST_length(ST_Transform(ST_Intersection(b.buffer, l.wkb_geometry), 3857))) AS value
FROM 
	sample_location_buffers AS b, 
	line_features AS l
WHERE 
	ST_Intersects(b.buffer, l.wkb_geometry)
GROUP BY 
	b.sensor_id, 
	l.geo_feature, 
	l.feature_type, 
	b.buffer_size;

--- polygon
DROP TABLE IF EXISTS polygon_geographic_features;
CREATE TABLE polygon_geographic_features AS
SELECT 
	b.sensor_id, 
	'polygon' AS geom_type, 
	pl.geo_feature, 
	pl.feature_type, 
	b.buffer_size, 
	SUM(ST_Area(ST_Transform(ST_Intersection(b.buffer, pl.wkb_geometry), 3857))) AS value
FROM 
	sample_location_buffers AS b, 
	polygon_features AS pl
WHERE
	ST_Intersects(b.buffer, pl.wkb_geometry)
GROUP BY 
	b.sensor_id, 
	pl.geo_feature, 
	pl.feature_type, 
	b.buffer_size;

--- merge into geographic_features
INSERT INTO geographic_features(sensor_id, geom_type, geo_feature, feature_type, buffer_size, value)
SELECT * FROM point_geographic_features
UNION
SELECT * FROM line_geographic_features
UNION
SELECT * FROM polygon_geographic_features;



