-- Task: Creating spatial buffers for air quality sensor locations

--- create a table for buffers
DROP TABLE IF EXISTS sample_location_buffers;
CREATE TABLE sample_location_buffers(
	gid BIGSERIAL PRIMARY KEY,
	sensor_id INTEGER,
	lon DOUBLE PRECISION NOT NULL,
	lat DOUBLE PRECISION NOT NULL,
	buffer_size INTEGER NOT NULL,
	buffer geometry(Polygon,4326) NOT NULL);
CREATE INDEX "sample_location_buffers_buffer_idx" ON sample_location_buffers USING gist(buffer);




--- Compute and insert the buffers of size 100, 500 and 1,000 meters to the table
--- Note: transform 'epsg:4326' geometries to a coordinate system with a meter unit

INSERT INTO sample_location_buffers (sensor_id, lon, lat, buffer_size, buffer)
SELECT 
	sensor_id,
	lon,
	lat,
	100, -- radius of buffer (meters)
	ST_Buffer(
		ST_SetSRID(ST_MakePoint(sample_locations.lon, 
								sample_locations.lat), 4326)::geography,
		100, -- radius of buffer (meters)
		'quad_segs=8')::geometry -- buffer generated
FROM sample_locations;

INSERT INTO sample_location_buffers (sensor_id, lon, lat, buffer_size, buffer)
SELECT 
	sensor_id,
	lon,
	lat,
	500, -- radius of buffer (meters)
	ST_Buffer(
		ST_SetSRID(ST_MakePoint(sample_locations.lon, 
								sample_locations.lat), 4326)::geography,
		500, -- radius of buffer (meters)
		'quad_segs=8')::geometry -- buffer generated
FROM sample_locations;

INSERT INTO sample_location_buffers (sensor_id, lon, lat, buffer_size, buffer)
SELECT 
	sensor_id,
	lon,
	lat,
	1000, -- radius of buffer (meters)
	ST_Buffer(
		ST_SetSRID(ST_MakePoint(sample_locations.lon, 
								sample_locations.lat), 4326)::geography,
		1000, -- radius of buffer (meters)
		'quad_segs=8')::geometry -- buffer generated
FROM sample_locations;










