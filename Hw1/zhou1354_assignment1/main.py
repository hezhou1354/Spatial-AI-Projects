import argparse
import time
import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession

from sedona.register.geo_registrator import SedonaRegistrator
from pyspark.sql.types import StructType, IntegerType, DoubleType
from sedona.utils import KryoSerializer, SedonaKryoRegistrator

from sample_code.common_db import engine


schema_point = StructType() \
    .add("sensor_id", IntegerType(), False) \
    .add("lon", DoubleType(), False) \
    .add("lat", DoubleType(), False)



def gen_buffers(input_file, buffer_sizes):

    point_df = spark.read.option("header", True).schema(schema_point).csv(input_file)
    point_df.createOrReplaceTempView("points")
    # point_df.show(5)

    """ complete the function to generate buffers """
    point_df1 = spark.sql("SELECT sensor_id, lon, lat, ST_Point(lon, lat) AS geom_point FROM points")
    point_df1.createOrReplaceTempView("geom_points")
    # point_df1.printSchema()
    # point_df1.show(5)
    #

    for buffer_size in buffer_sizes:
        print(buffer_size)
        buffer_sub_df = spark.sql(
            f"""
                SELECT
                df1.sensor_id,
                df1.lon,
                df1.lat,
                {buffer_size} AS buffer_size,
                ST_FlipCoordinates(ST_Transform(ST_Buffer(ST_Transform(ST_FlipCoordinates(df1.geom_point), 
                                                          "epsg:4326", "epsg:3857"), 
                                                {buffer_size}),
                                   "epsg:3857", "epsg:4326")) AS buffer
                FROM geom_points AS df1
            """
        )
        buffer_sub_df.createOrReplaceTempView(f"buffers_{buffer_size}")
        # buffer_sub_df.show()
        #

    buffer_df = spark.sql(
        f"""
            SELECT * FROM buffers_{buffer_sizes[0]}
            UNION
            SELECT * FROM buffers_{buffer_sizes[1]}
            UNION
            SELECT * FROM buffers_{buffer_sizes[2]}
        """
    )
    buffer_df.createOrReplaceTempView("buffers")
    buffer_df.show()
    #







def gen_geographic_features(osm_table, out_path):

    osm_df = pd.read_sql(f"select geo_feature, feature_type, wkb_geometry from {osm_table}", engine)
    osm_df = spark.createDataFrame(osm_df).persist()
    osm_df.createOrReplaceTempView("osm")
    # print(osm_df.rdd.getNumPartitions())

    """ compute geographic features for different geom """
    osm_df1 = spark.sql(
        """
            SELECT 
                geo_feature, 
                feature_type, 
                ST_GeomFromWKB(wkb_geometry) AS wkb_geometry 
            FROM 
                osm
        """
    )
    osm_df1.createOrReplaceTempView("geom_osm")
    # osm_df1.show(5)
    #

    if osm_table == 'polygon_features':
        geographic_feature_df = spark.sql(
            """
                SELECT 
                    b.sensor_id, 
                    'polygon' AS geom_type, 
                    o.geo_feature, 
                    o.feature_type, 
                    b.buffer_size, 
                    SUM(ST_Area(ST_Transform(ST_FlipCoordinates(ST_Intersection(b.buffer, o.wkb_geometry)), 
                                "epsg:4326", "epsg:3857"))) AS value
                FROM 
                    buffers AS b,
                    geom_osm AS o
                WHERE 
                    ST_Intersects(b.buffer, o.wkb_geometry)
                GROUP BY
                    b.sensor_id,
                    o.geo_feature,
                    o.feature_type,
                    b.buffer_size;
            """
        )
        geographic_feature_df.createOrReplaceTempView("geographic_features")
        # geographic_feature_df.show(5)
        #

    elif osm_table == 'line_features':
        geographic_feature_df = spark.sql(
            """
                SELECT 
                    b.sensor_id, 
                    'line' AS geom_type, 
                    o.geo_feature, 
                    o.feature_type, 
                    b.buffer_size, 
                    SUM(ST_Length(ST_Transform(ST_FlipCoordinates(ST_Intersection(b.buffer, o.wkb_geometry)),
                                  "epsg:4326", "epsg:3857"))) AS value
                FROM 
                    buffers AS b,
                    geom_osm AS o
                WHERE 
                    ST_Intersects(b.buffer, o.wkb_geometry)
                GROUP BY
                    b.sensor_id,
                    o.geo_feature,
                    o.feature_type,
                    b.buffer_size;
            """
        )
        geographic_feature_df.createOrReplaceTempView("geographic_features")
        # geographic_feature_df.show(5)
        #

    elif osm_table == "point_features":
        geographic_feature_df = spark.sql(
            """
                SELECT 
	                b.sensor_id, 
	                'point' AS geom_type, 
	                o.geo_feature, 
	                o.feature_type, 
	                b.buffer_size, 
	                COUNT(b.sensor_id) AS value
                FROM 
	                buffers AS b,
	                geom_osm AS o
                WHERE 
	                ST_Intersects(b.buffer, o.wkb_geometry)
	            GROUP BY
	                b.sensor_id,
                    o.geo_feature,
                    o.feature_type,
                    b.buffer_size;
            """
        )
        geographic_feature_df.createOrReplaceTempView("geographic_features")
        # geographic_feature_df.show(5)
        #


    else:
        raise NotImplementedError

    start_time = time.time()
    geographic_feature_df.coalesce(1).write.csv(f'{out_path}/{osm_table}_{int(time.time()/1000)}',
                                                header=True, sep=',')
    print(time.time() - start_time)
    # print(geographic_feature_df)


# input_file = "/Users/hezhou/Documents/Spring2022/CSci8980-003/Homework/Hw1/ca_purple_air_locations_subset.csv"
# out_path = "/Users/hezhou/Documents/Spring2022/CSci8980-003/Homework/Hw1"
# buffer_sizes = [100, 500, 1000]
# osm_table = 'line_features'

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, default='data/ca_purple_air_locations_subset.csv',
                        help='The path to the location file.')
    parser.add_argument('--osm_table', type=str, default='polygon_features',
                        help='The OSM table to query.')
    parser.add_argument('--out_path', type=str, default='./results',
                        help='The output folder path.')
    args = parser.parse_args()

    conf = SparkConf(). \
        setMaster("local[*]"). \
        set("spark.executor.memory", '4g'). \
        set("spark.driver.memory", '16g')

    spark = SparkSession. \
        builder. \
        appName("hw1"). \
        config(conf=conf). \
        config("spark.serializer", KryoSerializer.getName). \
        config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
        getOrCreate()

    SedonaRegistrator.registerAll(spark)

    gen_buffers(args.input_file, buffer_sizes=[100, 500, 1000])
    gen_geographic_features(osm_table=args.osm_table, out_path=args.out_path)

    spark.stop()

