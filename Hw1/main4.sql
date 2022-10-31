/*
CREATE INDEX "line_features_wkb_geometry_geom_idx" ON line_features USING gist(wkb_geometry);
CREATE INDEX "point_features_wkb_geometry_geom_idx" ON point_features USING gist(wkb_geometry);
CREATE INDEX "polygon_features_wkb_geometry_geom_idx" ON polygon_features USING gist(wkb_geometry);
*/
-- create a table for geographic features
/*
DROP TABLE IF EXISTS geographic_features;
CREATE TABLE geographic_features(
	gid BIGSERIAL PRIMARY KEY,
	sensor_id INTEGER NOT NULL,
	geom_type TEXT NOT NULL,
	geo_feature TEXT NOT NULL,
	feature_type TEXT NOT NULL,
	buffer_size INTEGER NOT NULL,
	value  DOUBLE PRECISION);
*/

-- insert geographic features into the geographic_features table
-- INSERT INTO geographic_features
-- # code block # --
--- point
/*
DROP TABLE IF EXISTS point_before_agg;
CREATE TABLE point_before_agg AS
SELECT 
	b.sensor_id, 
	'point' AS geom_type, 
	p.geo_feature, 
	p.feature_type, 
	b.buffer_size, 
	1 AS value
FROM 
	sample_location_buffers AS b, 
	point_features AS p
WHERE 
	ST_Intersects(b.buffer, p.wkb_geometry);



DROP TABLE IF EXISTS point_geographic_features;
CREATE TABLE point_geographic_features AS
SELECT 
	a.sensor_id, 
	a.geom_type, 
	a.geo_feature, 
	a.feature_type, 
	a.buffer_size, 
	SUM(a.value) AS value
FROM 
	point_before_agg AS a
GROUP BY 
	a.sensor_id, 
	a.geom_type, 
	a.geo_feature, 
	a.feature_type, 
	a.buffer_size;
*/

--- line
/*
DROP TABLE IF EXISTS line_before_agg;
CREATE TABLE line_before_agg AS
SELECT 
	b.sensor_id, 
	'line' AS geom_type, 
	l.geo_feature, 
	l.feature_type, 
	b.buffer_size, 
	ST_length(ST_Intersection(b.buffer, l.wkb_geometry)::geography) AS value
FROM 
	sample_location_buffers AS b, 
	line_features AS l
WHERE 
	ST_Intersects(b.buffer, l.wkb_geometry);



DROP TABLE IF EXISTS line_geographic_features;
CREATE TABLE line_geographic_features AS
SELECT 
	a.sensor_id, 
	a.geom_type, 
	a.geo_feature, 
	a.feature_type, 
	a.buffer_size, 
	SUM(a.value) AS value
FROM 
	line_before_agg as a
GROUP BY 
	a.sensor_id, 
	a.geom_type, 
	a.geo_feature, 
	a.feature_type, 
	a.buffer_size;
*/

--- polygon

DROP TABLE IF EXISTS polygon_before_agg;
CREATE TABLE polygon_before_agg AS
SELECT 
	b.sensor_id, 
	'polygon' AS geom_type, 
	pl.geo_feature, 
	pl.feature_type, 
	b.buffer_size, 
	ST_Area(ST_Intersection(b.buffer, pl.wkb_geometry)::geography) AS value
FROM 
	sample_location_buffers AS b, 
	polygon_features AS pl
WHERE
	ST_Intersects(b.buffer, pl.wkb_geometry);



DROP TABLE IF EXISTS polygon_geographic_features;
CREATE TABLE polygon_geographic_features AS
SELECT 
	a.sensor_id, 
	a.geom_type, 
	a.geo_feature, 
	a.feature_type, 
	a.buffer_size, 
	SUM(a.value) AS value
FROM 
	polygon_before_agg as a
GROUP BY 
	a.sensor_id, 
	a.geom_type, 
	a.geo_feature, 
	a.feature_type, 
	a.buffer_size;


-- merge into one
/*
INSERT INTO geographic_features(sensor_id, geom_type, geo_feature, feature_type, buffer_size, value)
SELECT * FROM point_geographic_features
UNION
SELECT * FROM line_geographic_features
UNION
SELECT * FROM polygon_geographic_features
*/



