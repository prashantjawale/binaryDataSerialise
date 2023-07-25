import geopandas as gpd
from geojson import Point, Feature, FeatureCollection, dump, MultiPolygon
import time
import os.path
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import shapely.wkt

from fastavro import writer, reader, parse_schema
from fastavro.schema import load_schema

import experiment3_pb2
from decimal import *
import numpy as np

getcontext().prec = 16
file_name = "experiment3"
avro_schema_file_name = "clc"

ROOT = "."
#ROOT = "/home/peter/Downloads/FIRENZE_CLC"
INPUT_GPKG_FILE = '{}/input-data/Firenze_land-use_CLC_WGS_84_SELECTED_10.gpkg'.format(ROOT)
#INPUT_GPKG_FILE = '{}/input-data/Firenze_land-use_CLC_GB-West.gpkg'.format(ROOT)
#INPUT_GPKG_FILE_LAYER = 'FI_Addresses_Sample_Aug23rd2021'

INPUT_GPKG_FILE_LAYER = 'Firenze_land-use_CLC_WGS_84_SELECTED_10'
#INPUT_GPKG_FILE_LAYER = 'Firenze_land-use_CLC_GB-West'
geopkg_geojson_timing = []
geojson_pbf_timing = []
geojson_avro_timing = []
avro_geojson_timing = []
pbf_geojson_timing = []
load_geojson_timing = []
reduce_geojson_timing = []
load_reduced_geojson_timing = []


## Read the GeoPackage using GeoPandas and convert to GeoJSON file.

tic = time.perf_counter()
print ("===========GeoPackage to GeoJSON==============")
print ("Begin: Converting GeoPackage to GeoJSON...")

file_size = os.path.getsize(INPUT_GPKG_FILE)
print("GeoPackge File size is {} Kb".format(round(file_size/1024),2))

clc_gdf = gpd.read_file(INPUT_GPKG_FILE, layer=INPUT_GPKG_FILE_LAYER)
clc_gdf.to_file("{}/geojson-output/{}.geojson".format(ROOT,file_name), driver='GeoJSON')
print ("End: Converting GeoPackage to GeoJSON...")



toc = time.perf_counter()

geopkg_geojson_timing.append(toc - tic)
print(f"Timing: Converting GeoPackage to GeoJSON : {toc - tic:0.4f} seconds")
file_size = os.path.getsize("{}/geojson-output/{}.geojson".format(ROOT,file_name))
print("GeoJSON File size is {} Kb".format(round(file_size/1024),2))


tic = time.perf_counter()
print ("\nBegin: Loading original GeoJSON file for processing...")
geojson_data = gpd.read_file("{}/geojson-output/{}.geojson".format(ROOT,file_name))
## obtain the CRS of the data from GeoPandas.
## This is important if the CRS is not WGS 84 (EPSG:4326).
## Without the CRS specified, a GIS such as QGIS cannot render the GeoJSON file correctly.
data_CRS = geojson_data.crs
print ("Data CRS {}".format(data_CRS))
toc = time.perf_counter()
load_geojson_timing.append(toc - tic)
print(f"Timing: Load GeoJSON file (using GeoPandas) : {toc - tic:0.4f} seconds")
print ("GeoJSON file: CRS {}".format(geojson_data.crs))
print ("GeoJSON file: Total Geometry Objects: {}".format(len(geojson_data['geometry'])))
print ("GeoJSON file: Dataset Properties: {}, List: {}".format(len(list(geojson_data)),list(geojson_data)))
print ("End: Loading GeoJSON file for processing...")


###########################################################################
## ATTRIBUTES or Properties
## There is 30 properties in the input file. Let's just select a subset of
## these for processing as many are redundant.
print ("========= GeoJSON Reducing Properties ===========")
tic = time.perf_counter()
print ("\nBegin: Create GeoJSON file with reduced number of properties...")
geoJSON_reducedProperties_read = []
for index, row in geojson_data.iterrows():

    reducedProperties = {}
    reducedProperties["pk_uid"] = row["pk_uid"]
    reducedProperties["regione"] = row["regione"]
    reducedProperties["nome"] = row["nome"]
    reducedProperties["siglaprov"] = row["siglaprov"]
    reducedProperties["siglareg"] = row["siglareg"]
    reducedProperties["codistat"] = row["codistat"]
    reducedProperties["ucs19"] = row["ucs19"]
    reducedProperties["c1_19"] = row["c1_19"]
    reducedProperties["c2_19"] = row["c2_19"]
    reducedProperties["c3_19"] = row["c3_19"]


    reducedPropertiesGeometry = row["geometry"].to_wkt() #stored as WKT
    # Convert to a shapely.geometry.polygon.Polygon object
    g1 = shapely.wkt.loads(reducedPropertiesGeometry)
    ## create the new feature with just the selected properties.
    geoJSON_reducedProperties_read.append(Feature(geometry=g1, properties=reducedProperties))

## we need to add the information about the CRS to the geojson file.
## if the data is not WSG:84/EPSG:4326 then this needs to be specified.
data_crs = {"type": "name","properties": {"name": "{}".format(data_CRS)}}

## create the FeatureCollection now.
geoJSON_feature_collection = FeatureCollection(geoJSON_reducedProperties_read,crs=data_crs)

with open('{}/geojson-output/{}_propReduced.geojson'.format(ROOT,file_name), 'w') as f:
   dump(geoJSON_feature_collection, f)

toc = time.perf_counter()
reduce_geojson_timing.append(toc - tic)
print(f"Timing: Create reduced properties GeoJSON file (using GeoPandas) : {toc - tic:0.4f} seconds")
print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
file_size = os.path.getsize("{}/geojson-output/{}_propReduced.geojson".format(ROOT,file_name))
print("File size is {} Kb".format(round(file_size/1024),2))


for test in range (0,1):

    tic = time.perf_counter()
    print ("\nBegin: Loading reduced GeoJSON file for processing...")
    geojson_data = gpd.read_file("{}/geojson-output/{}_propReduced.geojson".format(ROOT,file_name))
    ## obtain the CRS of the data from GeoPandas.
    ## This is important if the CRS is not WGS 84 (EPSG:4326).
    ## Without the CRS specified, a GIS such as QGIS cannot render the GeoJSON file correctly.
    data_CRS = geojson_data.crs
    print ("Data CRS {}".format(data_CRS))
    toc = time.perf_counter()
    load_reduced_geojson_timing.append(toc - tic)
    print(f"Timing: Load reduced GeoJSON file (using GeoPandas) : {toc - tic:0.4f} seconds")
    print ("GeoJSON file (reduced): CRS {}".format(geojson_data.crs))
    print ("GeoJSON file (reduced): Total Geometry Objects: {}".format(len(geojson_data['geometry'])))
    print ("GeoJSON file (reduced): Dataset Properties: {}, List: {}".format(len(list(geojson_data)),list(geojson_data)))
    print ("End: Loading reduced GeoJSON file for processing...")

    ###########################################################################

    #### PBF
    ## https://github.com/protocolbuffers/protobuf/issues/5450
    ## There are many good reasons why protobuf should steer clear of any validation of data inputs,
    ## even including making a field mandatory or required. Validation of values is a business logic decision,
    ## and one that cannot by solved by a data storage/interchange format.
    ## https://github.com/protocolbuffers/protobuf/issues/1606
    ## From the protobuf wire format we can tell whether a specific field exists or not.
    ## And in protobuf 2 generated code, all fields have a "HasXXX" method to tell whether the field exists or not in code.
    ## However in proto 3, we lost that ability.
    tic = time.perf_counter()
    print ("===========GeoJSON to PBF ==============")
    print ("Serialize GeoJSON to Protocol Buffer")

    polygons_list = experiment3_pb2.Experiment3Polygons()
    for index, row in geojson_data.iterrows():
        #fid =  row["fid"]
        nome = row["nome"]
        pk_uid = row["pk_uid"]
        ucs19 = row["ucs19"]
        c1_19 = row["c1_19"]
        c2_19 = row["c2_19"]
        c3_19 = row["c3_19"]
        regione = row["regione"]
        siglaprov = row["siglaprov"]
        siglareg = row["siglareg"]
        geometry = row["geometry"]
        codistat = row["codistat"]

        temp_polygon = polygons_list.experiment3.add()
        temp_polygon.pk_uid = pk_uid

        temp_polygon.siglareg = ""
        if (siglareg):
            temp_polygon.siglareg = siglareg

        temp_polygon.codistat = ""
        if (codistat):
            temp_polygon.codistat = codistat

        temp_polygon.siglaprov = ""
        if (siglaprov):
            temp_polygon.siglaprov = siglaprov

        temp_polygon.regione = ""
        if (regione):
            temp_polygon.regione = regione

        temp_polygon.c3_19 = ""
        if (c3_19):
            temp_polygon.c3_19 = c3_19
        temp_polygon.c2_19 = ""
        if (c2_19):
            temp_polygon.c2_19 = c2_19

        temp_polygon.c1_19 = ""
        if (c1_19):
            temp_polygon.c1_19 = c1_19
        temp_polygon.ucs19 = ""
        if (ucs19):
            temp_polygon.ucs19 = ucs19

        temp_polygon.nome = ""
        if (nome):
            temp_polygon.nome = nome

        ## these are usually not null.

        #temp_polygon.geometry = geometry.to_wkt()
        temp_polygon.geometry = str(geometry)
        #print ("Geometry WKT Str Len {}: ".format(len(geometry.to_wkt())))
        #print (geometry.to_wkt())

    f = open("{}/binary-output/{}.pbf".format(ROOT,file_name), "wb")
    f.write(polygons_list.SerializeToString())
    f.close()
    print ("Finished creating Protocol Buffer file")
    print ("Finished Serialize GeoJSON to Protocol Buffer")
    toc = time.perf_counter()
    geojson_pbf_timing.append(toc - tic)

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('{}/binary-output/{}.pbf'.format(ROOT,file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))

    #################################################################
    ########## Deserialize PBF to GeoJSON ###########################
    #################################################################

    ######################## protocol buffer ###########################
    print ("========= Protocol Buffer ===========")
    print ("Deserialize Protocol Buffer to GeoJSON")
    tic = time.perf_counter()

    # Reading data from serialized_file
    serialized_file= open("{}/binary-output/{}.pbf".format(ROOT,file_name), "rb")
    experiment3_polygons_read = experiment3_pb2.Experiment3Polygons()
    experiment3_polygons_read.ParseFromString(serialized_file.read())

    geoJSON_Features_read = []

    for protoBufObj in experiment3_polygons_read.experiment3:
        protoBufObj_properties = {}
        protoBufObj_properties["nome"] = protoBufObj.nome
        protoBufObj_properties["pk_uid"] = protoBufObj.pk_uid
        protoBufObj_properties["ucs19"] = protoBufObj.ucs19
        protoBufObj_properties["c1_19"] = protoBufObj.c1_19
        protoBufObj_properties["c2_19"] = protoBufObj.c2_19
        protoBufObj_properties["c3_19"] = protoBufObj.c3_19
        protoBufObj_properties["regione"] = protoBufObj.regione
        protoBufObj_properties["siglaprov"] = protoBufObj.siglaprov
        protoBufObj_properties["siglareg"] = protoBufObj.siglareg
        protoBufObj_properties["codistat"] = protoBufObj.codistat

        response_properties_geometry = protoBufObj.geometry #stored as WKT
        # Convert to a shapely.geometry.polygon.Polygon object
        g1 = shapely.wkt.loads(response_properties_geometry)

        geoJSON_Features_read.append(Feature(geometry=g1, properties=protoBufObj_properties))

    ## we need to add the information about the CRS to the geojson file.
    ## if the data is not WSG:84/EPSG:4326 then this needs to be specified.

    data_crs = {"type": "name","properties": {"name": "{}".format(data_CRS)}}

    ## create the FeatureCollection now.
    geoJSON_feature_collection = FeatureCollection(geoJSON_Features_read,crs=data_crs)

    with open('{}/geojson-output/{}-pbf.geojson'.format(ROOT,file_name), 'w') as f:
       dump(geoJSON_feature_collection, f)

    print ("Finished deserialize from Protocol Buffer to GeoJSON")
    toc = time.perf_counter()

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    timing_pbf_geojson = toc - tic
    pbf_geojson_timing.append(timing_pbf_geojson)

    file_size = os.path.getsize('{}/geojson-output/{}-pbf.geojson'.format(ROOT,file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))

    ################################################################
    ## fast avro
    print ("===========GeoJSON to Avro ==============")
    print ("Serialize GeoJSON to Apache Avro")
    fast_avro_clc_schema = load_schema("{}.avsc".format(avro_schema_file_name))

    tic = time.perf_counter()


    clc_fast_avro = []

    for index, row in geojson_data.iterrows():

        pk_uid = row["pk_uid"]
        regione = row["regione"]
        nome = row["nome"]
        siglaprov = row["siglaprov"]
        siglareg = row["siglareg"]
        codistat = row["codistat"]
        geometry = row["geometry"]
        ucs19 = row["ucs19"] #Codice uso suolo nell’anno 2019
        c1_19 = row["c1_19"]
        c2_19 = row["c2_19"]
        c3_19 = row["c3_19"]

        tempCLCAvro = {}
        tempCLCAvro["pk_uid"] = pk_uid
        tempCLCAvro["regione"] = regione
        tempCLCAvro["nome"] = nome
        tempCLCAvro["siglaprov"] = siglaprov
        tempCLCAvro["siglareg"] = siglareg
        tempCLCAvro["codistat"] = codistat
        tempCLCAvro["ucs19"] = row["ucs19"] #Codice uso suolo nell’anno 2019
        tempCLCAvro["c1_19"] = row["c1_19"]
        tempCLCAvro["c2_19"] = row["c2_19"]
        tempCLCAvro["c3_19"] = row["c3_19"]

        tempCLCAvro["geometry"] = str(geometry)

        clc_fast_avro.append(tempCLCAvro)


    with open("{}/binary-output/{}_fast.avro".format(ROOT,file_name), "wb") as out:
        writer(out,fast_avro_clc_schema,clc_fast_avro)


    print ("Finished serializing from GeoJSON to AVRO")
    toc = time.perf_counter()
    geojson_avro_timing.append(toc - tic)
    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize("{}/binary-output/{}_fast.avro".format(ROOT,file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))


    #############  Deserialize Fast Avro to GeoJSON #################
    ######################## Apache Avro ###########################
    print ("========= Apache Avro ===========")
    print ("Deserialize Fast Avro to GeoJSON")

    tic = time.perf_counter()
    geoJSON_Features_read = []
    with open('{}/binary-output/{}_fast.avro'.format(ROOT,file_name), "rb") as fastavro_fo:
        for t_address in reader(fastavro_fo):
            response_properties = {}
            response_properties["pk_uid"] = t_address["pk_uid"]
            response_properties["regione"] = t_address["regione"]
            response_properties["nome"] = t_address["nome"]
            response_properties["siglaprov"] = t_address["siglaprov"]
            response_properties["siglareg"] = t_address["siglareg"]
            response_properties["codistat"] = t_address["codistat"]
            response_properties["ucs19"] = t_address["ucs19"]
            response_properties["c1_19"] = t_address["c1_19"]
            response_properties["c2_19"] = t_address["c2_19"]
            response_properties["c3_19"] = t_address["c3_19"]
            response_properties_geometry = t_address["geometry"] #stored as WKT
            # Convert to a shapely.geometry.polygon.Polygon object
            g1 = shapely.wkt.loads(response_properties_geometry)

            geoJSON_Features_read.append(Feature(geometry=g1, properties=response_properties))

    ## we need to add the information about the CRS to the geojson file.
    ## if the data is not WSG:84/EPSG:4326 then this needs to be specified.

    data_crs = {"type": "name","properties": {"name": "{}".format(data_CRS)}}

    ## create the FeatureCollection now.
    geoJSON_feature_collection = FeatureCollection(geoJSON_Features_read,crs=data_crs)

    with open('{}/geojson-output/{}-avro_fast.geojson'.format(ROOT,file_name), 'w') as f:
       dump(geoJSON_feature_collection, f)


    toc = time.perf_counter()
    avro_geojson_timing.append(toc - tic)
    print ("Finished Deserialize Fast Avro to GeoJSON")

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('{}/geojson-output/{}-avro_fast.geojson'.format(ROOT,file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))


print ("\n\n\n==== Statistical Report ====")
pbf_geojson_timing_np = np.array(pbf_geojson_timing)
geojson_pbf_timing_np = np.array(geojson_pbf_timing)
geojson_avro_timing_np = np.array(geojson_avro_timing)
avro_geojson_timing_np = np.array(avro_geojson_timing)
geopkg_geojson_timing_np  = np.array(geopkg_geojson_timing)
load_geojson_timing_np  = np.array(load_geojson_timing)

print ("=====File Sizes=====")

file_size = os.path.getsize(INPUT_GPKG_FILE)
print("Input GPKG file size is {} Kb".format(round(file_size/1024),2))

file_size = os.path.getsize('{}/geojson-output/{}.geojson'.format(ROOT,file_name))
print("{}/geojson-output/{}.geojson size is {} Kb".format(ROOT,file_name,round(file_size/1024),2))

file_size = os.path.getsize('{}/geojson-output/{}_propReduced.geojson'.format(ROOT,file_name))
print("{}/geojson-output/{}_propReduced.geojson (Properties reduced) size is {} Kb".format(ROOT,file_name,round(file_size/1024),2))

file_size = os.path.getsize('{}/binary-output/{}.pbf'.format(ROOT,file_name))
print("{}/binary-output/{}.pbf size is {} Kb".format(ROOT,file_name,round(file_size/1024),2))
file_size = os.path.getsize('{}/binary-output/{}_fast.avro'.format(ROOT,file_name))
print("{}/binary-output/{}_fast.avro size is {} Kb".format(ROOT,file_name,round(file_size/1024),2))




file_size = os.path.getsize('{}/geojson-output/{}-avro_fast.geojson'.format(ROOT,file_name))
print("{}/geojson-output/{}-avro_fast.geojson (Properties reduced) size is {} Kb".format(ROOT,file_name,round(file_size/1024),2))

file_size = os.path.getsize('{}/geojson-output/{}-pbf.geojson'.format(ROOT,file_name))
print("{}/geojson-output/{}-pbf.geojson (Properties reduced) size is {} Kb".format(ROOT,file_name,round(file_size/1024),2))



print ("=====Run Times=====")
print("Load GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(load_geojson_timing_np, dtype=np.float64),np.std(load_geojson_timing_np, dtype=np.float64)))
print("Convert GPKG -> GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(geopkg_geojson_timing_np, dtype=np.float64),np.std(geopkg_geojson_timing_np, dtype=np.float64)))
print("Convert GeoJSON (reduced) -> PBF mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(geojson_pbf_timing, dtype=np.float64),np.std(geojson_pbf_timing, dtype=np.float64)))
print("Convert GeoJSON (reduced) -> AVRO mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(geojson_avro_timing, dtype=np.float64),np.std(geojson_avro_timing, dtype=np.float64)))
print("Convert PBF -> GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(pbf_geojson_timing, dtype=np.float64),np.std(pbf_geojson_timing, dtype=np.float64)))
print("Convert AVRO -> GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(avro_geojson_timing, dtype=np.float64),np.std(avro_geojson_timing, dtype=np.float64)))
