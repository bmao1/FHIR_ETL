import sys
import parameter
import os
from collections import OrderedDict
from annotation_func import buildBundle
import re

# edited from schema_gen
# string - > varchar
# data type struct<> ->row()
# remove colon : between column name and type, also on line 123, 153 hard coded colon
# backtick not supported, use double quote

def ext_flat_trim(str1):
    str2=""
    trim=0
    for i in str1.split("."):
        if i in ( "modifierExtension", "extension") and trim == 1:
            continue
        elif i in ( "modifierExtension", "extension"):
            str2 += i + "."
            trim = 1
        else :
            trim = 0
            str2 += i + "."

    str2 = re.sub(r'\.$', '', str2)
    return str2

def AWSbuildSchema(basePath, definitions, config={}, fullPath='', recursionList={}, colon= ' '):
    if 'includeExtensions' in  config:
        new_include_ext = []
        for i in config['includeExtensions']:
            i = ext_flat_trim(i)
            new_include_ext += [i]
        config['includeExtensions'] = new_include_ext
        
    schema=''
    typeMapping = { 'boolean': "BOOLEAN", 'integer': "INT", 'decimal': "DOUBLE", 'unsignedInt': "INT", 'positiveInt': "INT",\
		'fhirid': "varchar", 'string': "varchar", 'code': "varchar", 'uri': "varchar", 'url': "varchar", 'canonical': "varchar",\
		'base64Binary': "varchar", 'oid': "varchar", 'id': "varchar", 'uuid': "varchar", 'markdown': "varchar",\
		'instant': "varchar", 'date': "varchar", 'dateTime': "varchar", 'time': "varchar" }
    skip = ["Narrative", "markdown", "Resource"] 

    reserved_words = ["ALL", "ALTER", "AND", "ARRAY", "AS", "AUTHORIZATION", "BETWEEN", "BIGINT", "BINARY", "BOOLEAN", "BOTH", "BY", "CASE", "CASHE", "CAST", "CHAR", "COLUMN", "CONF", "CONSTRAINT", "COMMIT", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURSOR", "DATABASE", "DATE", "DAYOFWEEK", "DECIMAL", "DELETE", "DESCRIBE", "DISTINCT", "DOUBLE", "DROP", "ELSE", "END", "EXCHANGE", "EXISTS", "EXTENDED", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FLOAT", "FLOOR", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "FUNCTION", "GRANT", "GROUP", "GROUPING", "HAVING", "IF", "IMPORT", "IN", "INNER", "INSERT", "INT", "INTEGER,INTERSECT", "INTERVAL", "INTO", "IS", "JOIN", "LATERAL", "LEFT", "LESS", "LIKE", "LOCAL", "MACRO", "MAP", "MORE", "NONE", "NOT", "NULL", "NUMERIC", "OF", "ON", "ONLY", "OR", "ORDER", "OUT", "OUTER", "OVER", "PARTIALSCAN", "PARTITION", "PERCENT", "PRECEDING", "PRECISION", "PRESERVE", "PRIMARY", "PROCEDURE", "RANGE", "READS", "REDUCE", "REGEXP,REFERENCES", "REVOKE", "RIGHT", "RLIKE", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "SELECT", "SET", "SMALLINT", "START,TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP", "TO", "TRANSFORM", "TRIGGER", "TRUE", "TRUNCATE", "UNBOUNDED,UNION", "UNIQUEJOIN", "UPDATE", "USER", "USING", "UTC_TIMESTAMP", "VALUES", "VARCHAR", "VIEWS", "WHEN", "WHERE", "WINDOW", "WITH"]
    definitions = OrderedDict(sorted(definitions.items()))   #sorting definitions

    for path in definitions:
        if path.startswith(basePath) and path.split(".")[0] == basePath.split(".")[0] and len(path.split(".")) == len(basePath.split("."))+1:

            if definitions[path]['type'] == "ContentReference": 
                path = definitions[path]['contentReference']
            
            #create recursion list copy to pass into recursive function
            recursionList_copy= recursionList.copy()
            
            
            #skip same path happened in looped recursion over limit
            if path in recursionList_copy:
                recursionList_copy[path] = recursionList_copy[path] +1
            else :
                recursionList_copy[path] = 1
            maxRecursion = 3
            if 'recursionLimits' in config:
                if path in config['recursionLimits']:
                    maxRecursion = config['recursionLimits'][path]
            elif 'defaultRecursionLimit' in config:
                maxRecursion = config['defaultRecursionLimit']
            if recursionList_copy[path] > maxRecursion:
                #print(path + ": over limit {0}".format(maxRecursion))
                continue
                

            name = path.split(".").pop(-1)
            
            if len(fullPath) >0:
                elemFullPath = fullPath + "." + name
            else :
                elemFullPath = path # if fullPath, fullPath + "." + name
            definition = definitions[path]


            #include extension only is specified; remove extension in extension; remove "skip" types
            if path.startswith("Extension") and not any(x.find(elemFullPath) > -1 for x in config['includeExtensions']):
                continue
            #if 'inExtension' in config and config['inExtension']==True and definition['type'] =='Extension':
            #    continue
            if definition['type'] in skip:
                continue


            if name.upper() in reserved_words:
                    name = '"' + name + '"'
                    
            if definition['type'] in typeMapping:
                sqlType = typeMapping[definition['type']]
                
                if 'isArray' in definition and definition['isArray']==True:
                    addschema = name + colon + "array<{0}>,".format(sqlType)
                else:
                    addschema = name + colon + "{0},".format(sqlType)
                schema = schema + addschema

    
            elif definition['type'] in ["BackboneElement", "Element"]:
                fields = AWSbuildSchema(path, definitions, config, elemFullPath, recursionList_copy, ' ')
                
                if 'isArray' in definition and definition['isArray']==True:
                	addschema = "{0}{1} array<row({2})>,".format(name,colon,fields)
                else:
                	addschema = "{0}{1} row({2}),".format(name,colon,fields)
                schema = schema + addschema
                    
            elif definition['type'] == 'Extension':
                newConfig ={**config, 'defaultRecursionLimit': 1}
                if 'inExtension' in config and config['inExtension'] ==True:
                    newConfig.update({'inExtension':True})

                fields = AWSbuildSchema(definition['type'], definitions, newConfig, elemFullPath, recursionList_copy, ' ')
                if len(fields) >0:
                    
                    if 'isArray' in definition and definition['isArray']==True:
                        addschema = "{0}{1} array<row({2}, parent varchar, url varchar)>,".format(name,colon,fields)
                    else:
                    	addschema = "{0}{1} row({2}, parent varchar, url varchar),".format(name,colon,fields)
                    schema = schema + addschema
                    
            else :
                if 'inExtension' in config and config['inExtension']==True:
                    newConfig = {**config, 'defaultRecursionLimit': 1, 'inExtension':True}
                elif "recursionLimits" in config and "inExtension" in config:
                    newConfig = {**config, 'defaultRecursionLimit': config['recursionLimits'], 'inExtension':config["inExtension"]}
                elif "inExtension" in config:
                    newConfig = {**config,'inExtension':config["inExtension"]}
                else :
                    newConfig = config.copy()

                fields = AWSbuildSchema(definition['type'], definitions, newConfig, elemFullPath, recursionList_copy, ' ')

                if len(fields) >0:
                    if 'isArray' in definition and definition['isArray']==True:
                        addschema = "{0}{1} array<row({2})>,".format(name,colon,fields)
                    else:
                        addschema = "{0}{1} row({2}),".format(name,colon,fields)
                    schema = schema + addschema


            # annotation for query performance
            if 'includeAA' in config and config['includeAA'] == True:
                #remove `` from reserved words
                name = name.replace('"', '')
                if definition['type'] in ("date", "dateTime", "instant"):
                    fields = "start varchar, \"end\" varchar"
                    if 'isArray' in definition and definition['isArray']==True:
                        addschema = "{0}_aa{1} array<row({2})>,".format(name,colon,fields)
                    else :
                        addschema = "{0}_aa{1} row({2}),".format(name,colon,fields)
                    schema = schema + addschema
                    
                elif path == "Reference.reference":
                    addschema = "reference_id_aa {0} varchar, reference_prev_aa {1} varchar,".format(colon,colon)
                    schema = schema + addschema
                elif definition['type']== "string" and path != "Extension.parent" and name != "resourceType" and name != "reference":
                    if 'isArray' in definition and definition['isArray']==True:
                        addschema = "{0}_aa{1} array<varchar>,".format(name,colon)
                    else:
                        addschema = "{0}_aa{1} varchar,".format(name,colon)
                    schema = schema + addschema
                elif name =="resourceType":
                    addschema = "id_prev_aa{0} varchar,".format(colon)
                    schema = schema + addschema
    schema = schema[:-1]
    return schema


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
	    with open(os.getcwd() + "/schema/presto_" + eachresource.lower() +'.txt', 'w') as outfile:
	        outfile.write(str(awsschema))


if __name__ == '__main__':
    main(sys.argv[1:])




