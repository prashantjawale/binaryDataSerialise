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

#import address_pb2
from decimal import *
import numpy as np

getcontext().prec = 16
file_name = "experiment3"
avro_schema_file_name = "clc"
INPUT_GPKG_FILE = './input-data/Firenze_land-use_CLC_WGS_84_SELECTED.gpkg'
#INPUT_GPKG_FILE = 'test-geopackage.gpkg'
#INPUT_GPKG_FILE_LAYER = 'FI_Addresses_Sample_Aug23rd2021'

INPUT_GPKG_FILE_LAYER = 'Firenze_land-use_CLC_WGS_84_SELECTED'

geopkg_geojson_timing = []
geojson_pbf_timing = []
geojson_avro_timing = []
avro_geojson_timing = []
pbf_geojson_timing = []
load_geojson_timing = []

for test in range (0,1):

    ## Read the GeoPackage using GeoPandas and convert to GeoJSON file.

    tic = time.perf_counter()
    print ("===========GeoPackage to GeoJSON==============")
    print ("Begin: Converting GeoPackage to GeoJSON...")

    file_size = os.path.getsize(INPUT_GPKG_FILE)
    print("GeoPackge File size is {} Kb".format(round(file_size/1024),2))

    finland_gdf = gpd.read_file(INPUT_GPKG_FILE, layer=INPUT_GPKG_FILE_LAYER)
    finland_gdf.to_file("./geojson-output/{}.geojson".format(file_name), driver='GeoJSON')
    print ("End: Converting GeoPackage to GeoJSON...")



    toc = time.perf_counter()

    geopkg_geojson_timing.append(toc - tic)
    print(f"Timing: Converting GeoPackage to GeoJSON : {toc - tic:0.4f} seconds")
    file_size = os.path.getsize("./geojson-output/{}.geojson".format(file_name))
    print("GeoJSON File size is {} Kb".format(round(file_size/1024),2))

    tic = time.perf_counter()
    print ("\nBegin: Loading GeoJSON file for processing...")
    geojson_data = gpd.read_file("./geojson-output/{}.geojson".format(file_name))
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

    outfile = open("geojson_properties.csv","w")

    outfile.write("PK_UID,Regione,Nome,Siglaprov,Siglareg,Codistat,ucs19,c1_19,c2_19,c3_19\n")

    for index, row in geojson_data.iterrows():

        thisRow = []
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


        thisRow.append(pk_uid)
        thisRow.append(regione)
        thisRow.append(nome)
        thisRow.append(siglaprov)
        thisRow.append(siglareg)
        thisRow.append(codistat)
        thisRow.append(ucs19)
        thisRow.append(c1_19)
        thisRow.append(c2_19)
        thisRow.append(c3_19)
        thisRow = [str(x) for x in thisRow ]
        outfile.write(",".join(thisRow) + "\n")

    outfile.close()
