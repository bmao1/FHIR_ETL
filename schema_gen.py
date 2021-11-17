import sys
import parameter
from schema_func import AWSbuildSchema
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
	    awsschema = AWSbuildSchema(eachresource, definitions['definitions'], parameter.config, '', {})
	    with open(os.getcwd() + "/schema/" + eachresource.lower() +'.txt', 'w') as outfile:
	        outfile.write(str(awsschema))


if __name__ == '__main__':
    main(sys.argv[1:])




