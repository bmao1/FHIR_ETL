from glob import glob
import sys
import boto3
import os

def main(args):
    if len(args) < 2:
        sys.stderr.write('2 required arguments: <input dir> <resource list>. Nothing is done here.\n')
        sys.exit(0)
    
    s3_data_input= args[0]+"/output"
    athena_log = args[0]+"/test"
    res_list = args[1]


    #def create_athena_table(s3_data_input , schema_bucket ,schema_folder,**context ):
    #encrypt keys in airflow
    
    s3 = boto3.resource('s3')
    athena = boto3.client('athena')

    try:
        new_tables = res_list.split(',')
        print("New table will be created for {0}".format(new_tables))
    except:
        print("No new table for this run")

    for resource in new_tables:
        #point to s3 location for athena schema for resources
        filepath = os.getcwd() + '/schema/' + resource.lower() + '.txt'
        with open(filepath, 'r') as f:
            resource_schema = f.read()
    
            #drop old table/schema
            query = r'''DROP TABLE IF EXISTS {0};'''.format(resource.lower())
            athena.start_query_execution(QueryString=query, ResultConfiguration={'OutputLocation': athena_log})
            print("dropped table {0}".format(resource.lower()))
    
            #create new external table with new schema
            query = r'''CREATE EXTERNAL TABLE IF NOT EXISTS {0} ({1})
                ROW FORMAT serde 'org.openx.data.jsonserde.JsonSerDe'
                LOCATION '{2}/{3}/';
                '''.format(resource.lower(), resource_schema, s3_data_input,resource.lower())
            athena.start_query_execution(QueryString=query, ResultConfiguration={'OutputLocation': athena_log})
            print("Created external table {0}".format(resource.lower()))




if __name__ == '__main__':
    main(sys.argv[1:])
