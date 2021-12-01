
import pandas as pd
import json
import sys
import os
import re

def read_config(path):
    df = pd.read_csv(path, sep = ",", header = 0)
    json_df = json.loads(df.to_json(orient="records"))

    schemagen_extensions = []
    deid_fhirPathRules = ['{"path": "start_of_the_custom_config", "method": "redact"}']

    for i in json_df:
        # configure for MS de-id profile
        add_rule = {"path": i['path'], "method": i['method'] }
        deid_fhirPathRules += [json.dumps(add_rule)]
        
        # configure for DB schema profile
        if i['path'].startswith("nodesByType") or i['path'].endswith(".url") or i['path'].lower().find("extension") == -1:
            continue
        else:
            schemagen_extensions += [i['path']]

    deid_fhirPathRules += ['{"path": "end_of_the_custom_config", "method": "redact"}']        
    return deid_fhirPathRules, schemagen_extensions


def update_deid_config(path, config):
    with open(path) as f:
        data = f.readlines()
        newconfig = ""
        findconfig = 0
        for line in data:
            line = line.strip()
            try:
                line_path = json.loads(line.rstrip(','))
                if line_path["path"]== "start_of_the_custom_config":
                    print("Find de-id config")
                    findconfig = 1
                elif findconfig == 1:
                    if line_path["path"]== "end_of_the_custom_config":
                        findconfig = 0
                        for i in config:
                            newconfig = newconfig + str(i) + ",\n"
                        print("Replaced custom de-id config")
                else:
                    newconfig += line + "\n"
            except:
                if len(line) >0:
                    newconfig += line + "\n"

    with open(path,'w') as f:
        f.write(newconfig)


def update_schema_config(path, config):
    with open(path) as f:
        data = f.readlines()
        newconfig = ""
        findconfig = 0
        for line in data:
            line = line.strip()
            if line.startswith("extensions=["):
                 print("Find schema config")
                 if line.endswith("]"):
                     newconfig = newconfig + "extensions=" + str(config) + "\n"
                     print("Replaced schema config")
                 else:
                     findconfig = 1
            elif findconfig == 1 and line.strip().endswith("]"):
                findconfig = 0
                newconfig = newconfig + "extensions=" + str(config).replace(",", ",\n") + "\n"
                print("Replaced schema config")
            elif findconfig == 1:
                continue
            else:
                newconfig += str(line) + "\n"
                
    with open(path,'w') as f:
        f.write(newconfig)


def main(args):
    if len(args) ==0 :
        path = os.getcwd()
    else:
        print(" "+ args[0])
        path = re.sub(r'\/$', '', args[0]) 
    print('Read/write files in ' + path)

    config_mod= read_config(path+ "/custom_ext_config.csv")

    deid_config = path+ "/configuration-sample.json"
    if os.path.exists(deid_config):
        update_deid_config(deid_config, config_mod[0])
        print("De-id configure file is updated in " + deid_config)
    else:
        print("De-id configure file is ## not found ##")

    schema_config = path+ "/parameter.py"
    if os.path.exists(schema_config):
        update_schema_config(schema_config, config_mod[1])
        print("Schema configure file is updated in " + schema_config)
    else:
        print("Schema configure file is ## not found ##")



if __name__ == '__main__':
    main(sys.argv[1:])




















