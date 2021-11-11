#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import pyspark
from pyspark.sql import SparkSession
from delta import *
from delta.tables import *
from glob import glob
from pyspark.sql.functions import *
import time 
import re
import boto3
import numpy as np
from pyspark.sql.types import *


'''
ndjson==0.3.1
pyspark==3.1.2
delta-spark==1.0.0
boto3==1.18.21    # boto3==1.17.109
'''

## ******* AWS service role is required in Crawler to accesss Glue, s3 and Athena   *********

session = boto3.Session()
credentials = session.get_credentials()

# setup spark environment
# Loading ndjson into delta table 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 2g --packages "io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.1.2" pyspark-shell'

builder = SparkSession.builder.appName("test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", credentials.access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", credentials.secret_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

sc = pyspark.SparkContext.getOrCreate()
sc.setLogLevel("ERROR")


def ndjson_to_delta(filepath, deltapath):
    resource = deltapath.split("/")[-1].split(".")[0]
    #look for delta schema i n./schema/delta_*
    try:
        with open(os.getcwd()+ "/schema/delta_" + resource + ".txt") as f:
            schema = f.read()
            schema = eval(schema)
            print("Schema is found in {}".format(os.getcwd()+ "/schema/delta_" + resource.lower() + ".txt"))
    except:
        print("Schema for reading {} into spark df is not readily available. Creating temp schema ...".format(resource))
        import parameter
        from sparkschema_func import sparkBuildSchema
        from annotation_func import buildBundle
        resourceDefinitions = buildBundle(os.getcwd() + '/fhir/R4.0.1/profiles-resources.json')
        typeDefinitions = buildBundle(os.getcwd() + '/fhir/R4.0.1/profiles-types.json')
        definitions = {"definitions": {**resourceDefinitions['definitions'], **typeDefinitions['definitions']}, "resourceNames": resourceDefinitions['resourceNames']}
        schema = sparkBuildSchema(resource, definitions['definitions'], parameter.config, '', {})
        schema = "StructType([{}])".format(schema)
        schema = eval(schema)

    # will the data contains multiple version of the same resource?
    print("Reading file " + filepath)
    updatesDF = spark.read.json(filepath,schema).dropDuplicates(["id"])
    # try load delta table
    deltapath= deltapath.lower()
    try:
        deltaTable = DeltaTable.forPath(spark, deltapath)
        merge = 1
    except:
        print("Can not verified existing delta table in {}".format(deltapath))

    if merge =1:
        print("Merging into delta table in " + deltapath)
        ts = time()
        deltaTable.alias("delta").merge( \
            source = updatesDF.alias("updates"), \
            condition = "delta.id = updates.id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        print("Merge took {:0.1f}s. File {1} is merged into delta in {2}".format(time()-ts, filepath,deltapath))
    else:
        print("Creating new delta table in " + deltapath)
        updatesDF.write.option("overwriteSchema", "true").format("delta").mode("overwrite").save(deltapath)
        print("New delta table is created")

    deltaTable = DeltaTable.forPath(spark, deltapath)
    deltaTable.generate("symlink_format_manifest")
    print("Loaded {0} to {1} completed".format(filepath,deltapath))
    
    
def files_to_delta(filepath, deltapath):
    deltapath= re.sub('^s3:', 's3a:', deltapath)
    if os.path.isfile(filepath):
        ndjson_to_delta(filepath, deltapath)
    elif os.path.isdir(filepath):
        filepath = re.sub(r'\/$', '', filepath) +"/"
        print("... Reading files in \"{}\", assuming all files in this directory have the same schema".format(filepath))
        for fname in glob(filepath + '*.ndjson'):
            ndjson_to_delta(fname, deltapath)
    else:
        print("Filepath is incorrect")
        


#files_to_delta("/Users/binmao/Documents/synthea_old/output/fhir_ndjson/Patient.ndjson","s3a://schema-bintest2/patient")
    

#######################################################
# create table schema, update schema when needed, using the schema to create athena table 


# add a database to store schema, using system default aws credential

#glue = boto3.client('glue')



# aws crawler to table 

# use pre-defined service role
# glue_servicerole = 'service-role/AWSGlueServiceRole-bintest1'  #service role need to have access to glue, athena and s3
# s3filepath = "s3://schema-bintest2/delta/"


def create_crawler(crawler_name, craw_path, glue_servicerole, crawler_db="schema"):
    
    # create DB  if not exist
    athena = boto3.client('athena')
    query = '''CREATE DATABASE IF NOT EXISTS {};'''.format(crawler_db.lower())
    logpath = re.sub('\w+$','',craw_path) + "athena_logs"
    athena.start_query_execution(QueryString=query, ResultConfiguration={'OutputLocation': logpath})
    
    # create crawler
    glue = boto3.client('glue')
    try:
        glue.delete_crawler(Name=crawler_name)
        print("Existing Crawler {} is deleted ".format(crawler_name))
    except:
        print("... Creating crawler {}".format(crawler_name))
    
    response = glue.create_crawler(
        Name= crawler_name,
        Role= glue_servicerole,
        DatabaseName= crawler_db,
        Description='crawler to derive schema',
        Targets={
            'S3Targets': [
                {
                    'Path': craw_path,
                    'Exclusions': ["*.crc", "_SUCCESS", "_symlink**", "_delta**"],
                    'SampleSize': 5
                },
            ]
        },
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DELETE_FROM_DATABASE'
        }
        ,Configuration='{ "Version": 1.0, "Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas" } } }'
    )
    print("Crawler {} is created".format(crawler_name))
    return response

#create_crawler("pythonCreatedCrawler", craw_path="s3://schema-bintest2/patient", glue_servicerole = 'service-role/AWSGlueServiceRole-bintest1')

# check response 



def run_crawler(crawler_name):
    glue = boto3.client('glue')
    try:
        response_run = glue.start_crawler(Name=crawler_name)
        # print(response_run["ResponseMetadata"]["HTTPStatusCode"])
        if response_run["ResponseMetadata"]["HTTPStatusCode"] == 200 :
            print("Crawler {0} started at {1}".format(crawler_name,response_run["ResponseMetadata"]["HTTPHeaders"]["date"]))
        else :
            print("Failed to start crawler")
    except:
        print("Crawler {} error".format(crawler_name))

    
    try:
        timer = 1
        while True:
            response_get = glue.get_crawler(Name=crawler_name)
            status = response_get["Crawler"]["State"]
            if status == "READY":
                print("Crawler Run Completed")
                break
            else :
                print("Crawler is {}".format(response_get["Crawler"]["State"]))
                time.sleep(30 * timer)
                timer += 1
            if timer > 10:
                break
    except:
        print("Unable to check status of {}".format(crawler_name))
    
    return



# crawler_name = "pythonCreatedCrawler"
#run_crawler("pythonCreatedCrawler")


# create table from schema

def get_crawled_schema(crawler_tb, crawler_db = "schema"):
    glue = boto3.client('glue')
    try:
        response = glue.get_table(DatabaseName= crawler_db, Name= crawler_tb)
    except:
        print("Unable to get table info")
        return 
    schema = response["Table"]["StorageDescriptor"]["Columns"]

    reserved_words = ["ALL", "ALTER", "AND", "ARRAY", "AS", "AUTHORIZATION", "BETWEEN", "BIGINT", "BINARY", "BOOLEAN", "BOTH", "BY", "CASE", "CASHE", "CAST", "CHAR", "COLUMN", "CONF", "CONSTRAINT", "COMMIT", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURSOR", "DATABASE", "DATE", "DAYOFWEEK", "DECIMAL", "DELETE", "DESCRIBE", "DISTINCT", "DOUBLE", "DROP", "ELSE", "END", "EXCHANGE", "EXISTS", "EXTENDED", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FLOAT", "FLOOR", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "FUNCTION", "GRANT", "GROUP", "GROUPING", "HAVING", "IF", "IMPORT", "IN", "INNER", "INSERT", "INT", "INTEGER,INTERSECT", "INTERVAL", "INTO", "IS", "JOIN", "LATERAL", "LEFT", "LESS", "LIKE", "LOCAL", "MACRO", "MAP", "MORE", "NONE", "NOT", "NULL", "NUMERIC", "OF", "ON", "ONLY", "OR", "ORDER", "OUT", "OUTER", "OVER", "PARTIALSCAN", "PARTITION", "PERCENT", "PRECEDING", "PRECISION", "PRESERVE", "PRIMARY", "PROCEDURE", "RANGE", "READS", "REDUCE", "REGEXP,REFERENCES", "REVOKE", "RIGHT", "RLIKE", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "SELECT", "SET", "SMALLINT", "START,TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP", "TO", "TRANSFORM", "TRIGGER", "TRUE", "TRUNCATE", "UNBOUNDED,UNION", "UNIQUEJOIN", "UPDATE", "USER", "USING", "UTC_TIMESTAMP", "VALUES", "VARCHAR", "VIEWS", "WHEN", "WHERE", "WINDOW", "WITH"]
    reserved_words = reserved_words + ["div"]
    athena_schema= ""
    for col in schema:
        #add backtick to all column names
        athena_schema += "`" + col["Name"] + "` "
        
        #add backtick to reserved word in column type
        col_type = col["Type"]
        if ":" in col_type:
            namelist=re.findall(r'\w+',col_type)
            overlap = list(set(namelist) & set([x.lower() for x in reserved_words]))
            
            if len(overlap) > 0:
                # add "system" for testing only:  overlap += ["system"]
                # print(overlap)
                for word in overlap:
                    col_type= re.sub(r"([<,])({})(:)".format(word), r"\1`\2`\3", col_type)
        athena_schema +=  col_type + ","
    athena_schema = athena_schema[:-1]
    return athena_schema


#get_crawled_schema("patient")



def athena_delta_from_schema(athena_tb, schema, s3_data_input, logpath, athena_db = "default"):
    athena = boto3.client('athena')
    
    query = '''CREATE DATABASE IF NOT EXISTS {};'''.format(athena_db.lower())
    athena.start_query_execution(QueryString=query, ResultConfiguration={'OutputLocation': logpath})


    query = r'''DROP TABLE IF EXISTS {0}.{1};'''.format(athena_db.lower(),athena_tb.lower())
    athena.start_query_execution(QueryString=query, ResultConfiguration={'OutputLocation': logpath})
    # print("dropped table {0}.{1}".format(athena_db.lower(),athena_tb.lower()))
    
    #create new external table with new schema
    query = r'''CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} ({2})
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION '{3}/{1}/_symlink_format_manifest/';
        '''.format(athena_db.lower(), athena_tb.lower(), schema, s3_data_input)
        
    print(query)
    athena.start_query_execution(QueryString=query, ResultConfiguration={'OutputLocation': logpath})
    print("Created external table {0}.{1}".format(athena_db.lower(), athena_tb.lower()))
    

def schema_from_file(resource):
    with open(os.getcwd()+ "/schema/delta_" + resource.lower() + ".txt") as f:
        schema = f.read()
    return schema


#athena_delta_from_schema("patient", get_crawled_schema("patient"), "s3://schema-bintest2", 's3://s3-for-athena-bintest2/test', athena_db = "sampledb")





















