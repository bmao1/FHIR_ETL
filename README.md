# FHIR_ETL

A collection of code to run ETL process against Structured bulk FHIR data (ndjson) and clinical notes (txt).  
The ETL process and fhir schema is inspired by https://github.com/sync-for-science/a3  
This repo can run as a stand alone piece. It's also being tested and intended to be used with Cumulus pipeline https://github.com/smart-on-fhir/cumulus

Code list:  
Annotation: add annotation to structured bulk fhir. `python annotation.py {input_dir} {output_dir}`
- annotation.py
- annotation_func.py
- annotation_test.py
  
Load bulk to delta table. "athena_delta_upload_v1" (depreciated) use inferred schema while reading bulk fhir and use aws glue crawler to derive schema for athena external table. "athena_delta_upload_v2" use pre-defined schema to read bulk fhir into spark df and create Athena table. Both process use "merge into" (upsert) by id to keep latest record during subsequent data load. Run `python athena_delta_upload_v2.py {input_dir} {s3_datalake_path} {optional: list of resources}`. The third parameter "list of resources" is case sensitive, it should look like "Patient,Condition,Observation".
- athena_delta_upload_v1.py
- athena_delta_upload_v2.py
- delta_func.py
- parameter.py

Update athena table schema for fhir data and point to ndjson in s3
- athena_upload.py


Customize schema definition if needed. Mostly specify extensions and potential PII nodes to KEEP (such as patient race/ethnicity). It updates "configuration-sample.json" as well if file exists for De-id use. ()
List details in "custom_ext_config.csv" then run `python custom_config.py`
- custom_config.py
- custom_ext_config.csv
- parameter.py
- configuration-sample.json

FHIR resource profile files from HL7
- fhir

Generate schema for creating external table in Athena, base on resource profile files in "fhir"
- schema_func.py
- schema_gen.py
- schema_test.py
- schema
- fhir


Generate schema for spark df for reading bulk fhir, base on resource profile files in "fhir"
- sparkschema_func.py
- sparkschema_gen.py


Requirements
- requirements.txt
The spark 