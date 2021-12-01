#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from glob import glob
import sys
import boto3
import os
import re
import json
from delta_func import *

def main(args):
    if len(args) < 2:
        sys.stderr.write('2 arguments required : <input local dir> <s3 datalake path>, 1 optional <resource list>\n')
        sys.exit(-1)
    
    inputpath= re.sub(r'\/$', '', args[0]) +"/**/*.ndjson"
    s3lakepath= re.sub(r'\/$', '', args[1].lower()) +"/delta/"

    try:
        print("received " + args[2])
        new_tables = args[2].lower().split(',')
        print("New table schema will be created for {0}".format(new_tables))
    except:
        new_tables =[]
        print("No new table schema for this run")

   
    # loop through all files in local path, load to delta. 
    file_list = [f for f in glob(inputpath, recursive=True) if os.path.isfile(f)]
    updated_resource = []
    
    if len(file_list) >0 :
        for fname in file_list:
            with open(fname) as f:
                lineContent = json.loads(f.readline().rstrip())
                resourcename = lineContent["resourceType"]
            files_to_delta(fname, s3lakepath + resourcename)
            resourcename = resourcename.lower()
            if resourcename not in updated_resource:
                updated_resource += [resourcename]
        
    # update athena tables, use new schema from crawler if needed
    if len(updated_resource) > 0:
        for resourcename in updated_resource:
            schema = get_crawled_schema(resourcename)
            if schema is None:
                print("Crawl delta_{} does not exist. New crawler will be created".format(resourcename))
                if resourcename not in new_tables:
                    new_tables += [resourcename]
            
            if resourcename in new_tables:
                create_crawler( crawler_name = "delta_" + resourcename
                               , craw_path = s3lakepath + resourcename
                               , glue_servicerole = "service-role/AWSGlueServiceRole-bintest1"
                               , crawler_db="schema")
                run_crawler("delta_" + resourcename)
            athena_delta_from_schema( athena_tb = resourcename
                                     , schema = get_crawled_schema(resourcename)
                                     , s3_data_input = s3lakepath
                                     , logpath= s3lakepath + 'athena_logs'
                                     , athena_db = "delta")

        print("Table schema is updated in {}".format(new_tables))
        print("Table content is updated in {}".format(resourcename))            

if __name__ == '__main__':
    main(sys.argv[1:])




######################






