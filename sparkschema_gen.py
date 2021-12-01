import sys
import parameter
from sparkschema_func import sparkBuildSchema
from annotation_func import buildBundle
import os



def main(args):

	if len(sys.argv[1]) > 0:
	    resourceTypes = sys.argv[1].split(',')
	    print("New schema for : {0}".format(resourceTypes))
	else:
	    resourceTypes =[]


	resourceDefinitions = buildBundle(os.getcwd() + '/fhir/R4.0.1/profiles-resources.json')
	typeDefinitions = buildBundle(os.getcwd() + '/fhir/R4.0.1/profiles-types.json')
	definitions = {"definitions": {**resourceDefinitions['definitions'], **typeDefinitions['definitions']}, "resourceNames": resourceDefinitions['resourceNames']}

	for eachresource in resourceTypes:
	    print("Generating schema for : {0}".format(eachresource))
	    schema = sparkBuildSchema(eachresource, definitions['definitions'], parameter.config, '', {})
	    schema = "StructType([{}])".format(schema)
	    with open(os.getcwd() + "/schema/delta_" + eachresource.lower() +'.txt', 'w') as outfile:
	        outfile.write(str(schema))


if __name__ == '__main__':
    main(sys.argv[1:])




