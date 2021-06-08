
from glob import glob
import sys
import json
from annotation_func import *
import parameter

#sem_type_list =['DiseaseDisorderMention','SignSymptomMention','MedicationMention','ProcedureMention']
#resource_map={'DiseaseDisorderMention':'Condition', 'SignSymptomMention':'Observation', 'MedicationMention':'MedicationStatement','ProcedureMention':'Procedure'}



def main(args):
    if len(args) < 2:
        sys.stderr.write('2 required arguments: <input dir> <output file>\n')
        sys.exit(-1)
    
    inputdir= args[0]
    outputdir= args[1]
    
    resourceDefinitions = buildBundle(parameter.folder_loc + '/fhir/R4.0.1/profiles-resources.json')
    typeDefinitions = buildBundle(parameter.folder_loc + '/fhir/R4.0.1/profiles-types.json')
    definitions = {"definitions": {**resourceDefinitions['definitions'], **typeDefinitions['definitions']}, "resourceNames": resourceDefinitions['resourceNames']}
    

    # this is the old version without processing contained resource
    """for filename in glob(trimUrl(inputdir) + '/*.ndjson'):
        with open(filename) as f:
            resourcename = filename[:-7].split("/")[-1]
            content = f.readlines()
            for line in content:
                lineContent = json.loads(line)
                result = transformResource(lineContent, definitions["definitions"], "", "", {"includeExtensions": parameter.extensions})
                with open(trimUrl(outputdir) + '/' + resourcename + '.ndjson', 'a') as outfile:
                    outfile.write(str(result))
                    outfile.write('\n')"""

    for filename in glob(trimUrl(inputdir) + '/*.ndjson'):
        with open(filename) as f:
            resourcename = filename[:-7].split("/")[-1]
            content = f.readlines()
            for line in content:
                lineContent = json.loads(line)
                if "contained" in lineContent:
                    for cont_res in lineContent["contained"]:
                        result = transformResource(cont_res, definitions["definitions"], "", "", {"includeExtensions": parameter.extensions})
                        with open(trimUrl(outputdir) + '/' + cont_res["resourceType"] + '.ndjson', 'a') as outfile:
                            outfile.write(str(result))
                            outfile.write('\n') 
                    #remove contained contents after processing
                    del lineContent["contained"]
                
                #process the resource
                result = transformResource(lineContent, definitions["definitions"], "", "", {"includeExtensions": parameter.extensions})
                with open(trimUrl(outputdir) + '/' + resourcename + '.ndjson', 'a') as outfile:
                    outfile.write(str(result))
                    outfile.write('\n')                
                        

    

if __name__ == '__main__':
    main(sys.argv[1:])










