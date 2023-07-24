import geopandas as gpd
from geojson import Point, Feature, FeatureCollection, dump
import time
import os.path
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from google.protobuf.internal.encoder import _VarintBytes
from google.protobuf.internal.decoder import _DecodeVarint32

from fastavro import writer, reader, parse_schema
from fastavro.schema import load_schema

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as feather

import location_pb2
from decimal import *
import numpy as np
import pandas as pd
import webbrowser

getcontext().prec = 16
file_name = "experiment3"
avro_schema_file_name = "address"
INPUT_SHP_FILE = 'gis_osm_traffic_free_1.shp'

shp_geojson_timing = []
geojson_pbf_timing = []
geojson_parq_timing = []
geojson_feth_timing = []
geojson_avro_timing = []
avro_geojson_timing = []
pbf_geojson_timing = []
parq_geojson_timing = []
feth_geojson_timing = []
load_geojson_timing = []


for test in range (0,5):

    ## Read the SHP using GeoPandas and convert to GeoJSON file.

    tic = time.perf_counter()
    print ("===========ShapeFile to GeoJSON==============")
    print ("Begin: Converting ShapeFile to GeoJSON...")

    file_size = os.path.getsize(INPUT_SHP_FILE)
    print("Shp File size is {} Kb".format(round(file_size/1024),2))

    shp_gdf = gpd.read_file(INPUT_SHP_FILE)
    shp_gdf.to_file("./geojson-output/{}.geojson".format(file_name), driver='GeoJSON')
    print ("End: Converting ShapeFile to GeoJSON...")
    toc = time.perf_counter()

    shp_geojson_timing.append(toc - tic)
    print(f"Timing: Converting ShapeFile to GeoJSON : {toc - tic:0.4f} seconds")
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

    ## fast avro
    print ("===========GeoJSON to Avro ==============")
    print ("Serialize GeoJSON to Apache Avro")
    fast_avro_address_schema = load_schema("{}.avsc".format(avro_schema_file_name))

    tic = time.perf_counter()
    addresses_fast_avro = []

    for i in range(0,len(geojson_data)):
        tempAddressAvro = {col: geojson_data[col].iloc[i] for col in geojson_data.columns}
        tempAddressAvro["geometry"] = tempAddressAvro["geometry"].wkt
        tempAddressAvro["fid"] = 1

        addresses_fast_avro.append(tempAddressAvro)


    with open("./binary-output/{}_fast.avro".format(file_name), "wb") as out:
        writer(out,fast_avro_address_schema,addresses_fast_avro)


    print ("Finished serializing JSON to GeoJSON")
    toc = time.perf_counter()
    geojson_avro_timing.append(toc - tic)
    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize("./binary-output/{}_fast.avro".format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))



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

    shp_data = []
    for index, row in geojson_data.iterrows():
        address_list = location_pb2.location()
        geometry = row["geometry"].wkt
        address_list.geometry = geometry
        shp_data.append(address_list)

    serialized_data = b"".join([
        _VarintBytes(len(msg.SerializeToString())) + msg.SerializeToString()
        for msg in shp_data
    ])

    with open("./binary-output/{}.pbf".format(file_name), "wb") as f:
        f.write(serialized_data)
    f.close()
    print ("Finished creating Protocol Buffer file")
    toc = time.perf_counter()
    geojson_pbf_timing.append(toc - tic)

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./binary-output/{}.pbf'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))

    ### Apache Parquet
    tic = time.perf_counter()
    print ("===========GeoJSON to Parquet ==============")
    print ("Serialize GeoJSON to Parquet")

    df = geojson_data

    df["geometry"] = df["geometry"].apply(lambda point: point.wkt)

    # Convert pandas DataFrame to PyArrow table
    table = pa.Table.from_pandas(df)

    # Write the table to a Parquet file
    pq.write_table(table, './binary-output/{}_parq.parquet'.format(file_name))

    print ("Finished creating Apache parquet file")
    toc = time.perf_counter()
    geojson_parq_timing.append(toc - tic)

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./binary-output/{}_parq.parquet'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))


    ### Apache Feather

    tic = time.perf_counter()
    print ("===========GeoJSON to Feaather ==============")
    print ("Serialize GeoJSON to Feather")

    df = geojson_data

    #df["geometry"] = df["geometry"].apply(lambda point: point.wkt)

    # Convert pandas DataFrame to PyArrow table
    table = pa.Table.from_pandas(df)

    # Serialize the table to a Feather file format
    feather.write_feather(table, './binary-output/{}_fet.feather'.format(file_name))

    print ("Finished creating Apache Feather file")
    toc = time.perf_counter()
    geojson_feth_timing.append(toc - tic)

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./binary-output/{}_fet.feather'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))



    

    ######################## protocol buffer ###########################
    print ("========= Protocol Buffer ===========")
    print ("Deserialize Protocol Buffer to GeoJSON")
    tic = time.perf_counter()
    # Reading data from serialized_file
    serialized_file= open('./binary-output/{}.pbf'.format(file_name), "rb")
    data = serialized_file.read()

    geoJSON_Features_read = []
    deserialized_messages = []

    # Initialize the offset
    offset = 0

    while offset < len(data):
        # Read the size of the message
        msg_size, new_pos = _DecodeVarint32(data, offset)
        offset = new_pos

        # Create a new LocationMessage protobuf message
        location_message = location_pb2.location()

        # Parse the message from the binary data
        location_message.ParseFromString(data[offset:offset + msg_size])

        # Move the offset to the start of the next message
        offset += msg_size

        response_properties_geometry = location_message.geometry #stored as WKT

        s = gpd.GeoSeries.from_wkt([response_properties_geometry])
        # Geopandas GeoSeries converts an array or list of WKT to a GeoSeries list.
        # There is only one element in the list so we index at 0.
        #print (">> {},{}".format(Decimal(s[0].x),Decimal(s[0].y)))
        # rounded to 6 decimal places by default (GeoJSON package documentation)
        ## precision 10 seems to be the maximum allowed.
        geocoord_read = Point((float(s[0].x),float(s[0].y)),precision=10)
        geoJSON_Features_read.append(Feature(geometry=geocoord_read))

    ## we need to add the information about the CRS to the geojson file.
    ## if the data is not WSG:84/EPSG:4326 then this needs to be specified.

    data_crs = {"type": "name","properties": {"name": "{}".format(data_CRS)}}

    ## create the FeatureCollection now.
    geoJSON_feature_collection = FeatureCollection(geoJSON_Features_read,crs=data_crs)

    with open('./geojson-output/{}-pbf.geojson'.format(file_name), 'w') as f:
       dump(geoJSON_feature_collection, f)

    toc = time.perf_counter()
    pbf_geojson_timing.append(toc - tic)

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./geojson-output/{}-pbf.geojson'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))



    ######################## Apache Avro ###########################
    print ("========= Apache Avro ===========")
    print ("Deserialize Fast Avro to GeoJSON")

    tic = time.perf_counter()
    geoJSON_Features_read = []
    with open('./binary-output/{}_fast.avro'.format(file_name), "rb") as fastavro_fo:
        for t_address in reader(fastavro_fo):
            response_properties = {}
            response_properties_geometry = t_address["geometry"] #stored as WKT
            s = gpd.GeoSeries.from_wkt([response_properties_geometry])
            # Geopandas GeoSeries converts an array or list of WKT to a GeoSeries list.
            # There is only one element in the list so we index at 0.
            #print (">> {},{}".format(Decimal(s[0].x),Decimal(s[0].y)))
            # rounded to 6 decimal places by default (GeoJSON package documentation)
            ## precision 10 seems to be the maximum allowed.

            z = '{:<018}'

            geocoord_read = Point((float(s[0].x),float(s[0].y)),precision=10)
            geoJSON_Features_read.append(Feature(geometry=geocoord_read, properties=response_properties))

    ## we need to add the information about the CRS to the geojson file.
    ## if the data is not WSG:84/EPSG:4326 then this needs to be specified.

    data_crs = {"type": "name","properties": {"name": "{}".format(data_CRS)}}

    ## create the FeatureCollection now.
    geoJSON_feature_collection = FeatureCollection(geoJSON_Features_read,crs=data_crs)

    with open('./geojson-output/{}-avro_fast.geojson'.format(file_name), 'w') as f:
       dump(geoJSON_feature_collection, f)


    toc = time.perf_counter()
    avro_geojson_timing.append(toc - tic)

    print(f"Timing Information: Total:\n\tSerialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./geojson-output/{}-avro_fast.geojson'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))


    
    ######################## Apache Parquet ###########################
    print ("========= Apache Parquet ===========")
    print ("Deserialize Apache Parquet to GeoJSON")
    tic = time.perf_counter()
    # Reading data from serialized_file
    parquet_table = pq.read_table('./binary-output/{}_parq.parquet'.format(file_name))
    df = parquet_table.to_pandas()
    df['geometry'] = df['geometry'].apply(lambda x: Point((float(x[7:-1].split(' ')[0]), float(x[7:-1].split(' ')[1]))))
    df['geometry'] = df['geometry'].apply(lambda point: Point((float(point['coordinates'][0]), float(point['coordinates'][1]))))

    # Create the GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    gdf.to_file('./geojson-output/{}-parq.geojson'.format(file_name), driver='GeoJSON')

    toc = time.perf_counter()
    parq_geojson_timing.append(toc - tic)

    print(f"Timing Information: Total:\n\tDeserialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./geojson-output/{}-parq.geojson'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))


    ######################## Apache Feather ###########################
    print ("========= Apache Feather ===========")
    print ("Deserialize Apache Feather to GeoJSON")
    tic = time.perf_counter()
    # Reading data from serialized_file
    df = feather.read_feather('./binary-output/{}_fet.feather'.format(file_name))
    
    df['geometry'] = df['geometry'].apply(lambda x: Point((float(x[7:-1].split(' ')[0]), float(x[7:-1].split(' ')[1]))))
    df['geometry'] = df['geometry'].apply(lambda point: Point((float(point['coordinates'][0]), float(point['coordinates'][1]))))

    # Create the GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    gdf.to_file('./geojson-output/{}-fet.geojson'.format(file_name), driver='GeoJSON')

    toc = time.perf_counter()
    feth_geojson_timing.append(toc - tic)

    print(f"Timing Information: Total:\n\tDeserialization Process \n\tTotal: {toc - tic:0.4f} seconds")
    file_size = os.path.getsize('./geojson-output/{}-fet.geojson'.format(file_name))
    print("File size is {} Kb".format(round(file_size/1024),2))





print ("\n\n\n==== Statistical Report ====")
pbf_geojson_timing_np = np.array(pbf_geojson_timing)
geojson_pbf_timing_np = np.array(geojson_pbf_timing)
geojson_avro_timing_np = np.array(geojson_avro_timing)
avro_geojson_timing_np = np.array(avro_geojson_timing)
geojson_parq_timing_np = np.array(geojson_parq_timing)
parq_geojson_timing_np = np.array(parq_geojson_timing)
geojson_feth_timing_np = np.array(geojson_feth_timing)
feth_geojson_timing_np = np.array(feth_geojson_timing)
shp_geojson_timing_np  = np.array(shp_geojson_timing)
load_geojson_timing_np  = np.array(load_geojson_timing)

print ("=====File Sizes=====")

file_size = os.path.getsize(INPUT_SHP_FILE)
print("Input SHP file size is {} Kb".format(round(file_size/1024),2))

file_size = os.path.getsize('./geojson-output/{}.geojson'.format(file_name))
print("./geojson-output/{}.geojson size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./binary-output/{}_fast.avro'.format(file_name))
print("./geojson-output/{}_fast.avro size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./geojson-output/{}-avro_fast.geojson'.format(file_name))
print("./geojson-output/{}-avro_fast.geojson size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./binary-output/{}.pbf'.format(file_name))
print("./geojson-output/{}.pbf size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./geojson-output/{}-pbf.geojson'.format(file_name))
print("./geojson-output/{}-pbf.geojson size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./binary-output/{}_parq.parquet'.format(file_name))
print("./geojson-output/{}_parq.parquet size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./geojson-output/{}-parq.geojson'.format(file_name))
print("./geojson-output/{}-parq.geojson size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./binary-output/{}_fet.feather'.format(file_name))
print("./geojson-output/{}_fet.feather size is {} Kb".format(file_name,round(file_size/1024),2))

file_size = os.path.getsize('./geojson-output/{}-fet.geojson'.format(file_name))
print("./geojson-output/{}-fet.geojson size is {} Kb".format(file_name,round(file_size/1024),2))


print ("=====Run Times=====")
print("Convert SHP -> GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(shp_geojson_timing_np, dtype=np.float64),np.std(shp_geojson_timing_np, dtype=np.float64)))

print("Load GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(load_geojson_timing_np, dtype=np.float64),np.std(load_geojson_timing_np, dtype=np.float64)))

print("Serialize: GeoJSON->Avro mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(geojson_avro_timing_np, dtype=np.float64),np.std(geojson_avro_timing_np, dtype=np.float64)))
print("Serialize: GeoJSON->PBF mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(geojson_pbf_timing_np, dtype=np.float64),np.std(geojson_pbf_timing_np, dtype=np.float64)))
print("Serialize: GeoJSON->Parq mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(geojson_parq_timing_np, dtype=np.float64),np.std(geojson_parq_timing_np, dtype=np.float64)))
print("Serialize: GeoJSON->Feth mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(geojson_feth_timing_np, dtype=np.float64),np.std(geojson_feth_timing_np, dtype=np.float64)))
print("Deserialize: Avro->GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(avro_geojson_timing_np, dtype=np.float64),np.std(avro_geojson_timing_np, dtype=np.float64)))
print("Deserialize: PBF->GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(pbf_geojson_timing_np,dtype=np.float64),np.std(pbf_geojson_timing_np,dtype=np.float64)))
print("Deserialize: Parq->GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(parq_geojson_timing_np,dtype=np.float64),np.std(parq_geojson_timing_np,dtype=np.float64)))
print("Deserialize: feth->GeoJSON mean {:0.3f}s, std-dev {:0.4f}s".format(np.mean(feth_geojson_timing_np,dtype=np.float64),np.std(feth_geojson_timing_np,dtype=np.float64)))

data = pd.DataFrame({
    'Type': ['Original File','Serialize: GeoJSON->Avro','Serialize: GeoJSON->PBF','Serialize: GeoJSON->Parq mean','Serialize: GeoJSON->Feth','Deserialize: Avro->GeoJSON','Deserialize: PBF->GeoJSON','Deserialize: Parq->GeoJSON','Deserialize: feth->GeoJSON'],
    'fileSize':[os.path.getsize(INPUT_SHP_FILE),os.path.getsize('./binary-output/{}_fast.avro'.format(file_name)),os.path.getsize('./binary-output/{}.pbf'.format(file_name)), os.path.getsize('./binary-output/{}_parq.parquet'.format(file_name)),os.path.getsize('./binary-output/{}_fet.feather'.format(file_name)),os.path.getsize('./geojson-output/{}-avro_fast.geojson'.format(file_name)),os.path.getsize('./geojson-output/{}-pbf.geojson'.format(file_name)),os.path.getsize('./geojson-output/{}-parq.geojson'.format(file_name)),os.path.getsize('./geojson-output/{}-fet.geojson'.format(file_name))],
    'meanTime':[0, np.mean(geojson_avro_timing_np),np.mean(geojson_pbf_timing_np),np.mean(geojson_parq_timing_np),np.mean(geojson_feth_timing_np),np.mean(avro_geojson_timing_np),np.mean(pbf_geojson_timing_np),np.mean(parq_geojson_timing_np),np.mean(feth_geojson_timing_np)],
    'std-dev':[0, np.std(geojson_avro_timing_np),np.std(geojson_pbf_timing_np),np.std(geojson_parq_timing_np),np.std(geojson_feth_timing_np),np.std(avro_geojson_timing_np),np.std(pbf_geojson_timing_np),np.std(parq_geojson_timing_np),np.std(feth_geojson_timing_np)],
})

data['fileSize'] = data['fileSize'].apply(lambda x:  f"{round(x/1024, 2)} kb")
# Create CSS styles for the table
table_style = '''
<style>
    .decorative-table {
        border-collapse: collapse;
        width: 100%;
    }
    
    .decorative-table th, .decorative-table td {
        padding: 8px;
        text-align: left;
        border-bottom: 1px solid #ddd;
    }
    
    .decorative-table th {
        background-color: #f2f2f2;
    }
    
    .decorative-table tr:nth-child(even) {
        background-color: #f9f9f9;
    }
    
    .decorative-table tr:hover {
        background-color: #eaeaea;
    }
</style>
'''

# Convert DataFrame to HTML table
html_table = data.to_html(classes='decorative-table', index=False)

# Combine table style and HTML table
decorative_table = table_style + html_table

with open('results.html', 'w') as f:
    f.write(decorative_table)

webbrowser.open('results.html')
