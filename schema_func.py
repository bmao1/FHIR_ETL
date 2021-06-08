#!/usr/bin/env python3
# -*- coding: utf-8 -*-




from collections import OrderedDict
from annotation_func import buildBundle

def AWSbuildSchema(basePath, definitions, config={}, fullPath='', recursionList={}, colon= ' '):

    schema=''
    typeMapping = { 'boolean': "BOOLEAN", 'integer': "INT", 'decimal': "DOUBLE", 'unsignedInt': "INT", 'positiveInt': "INT",\
		'fhirid': "STRING", 'string': "STRING", 'code': "STRING", 'uri': "STRING", 'url': "STRING", 'canonical': "STRING",\
		'base64Binary': "STRING", 'oid': "STRING", 'id': "STRING", 'uuid': "STRING", 'markdown': "STRING",\
		'instant': "STRING", 'date': "STRING", 'dateTime': "STRING", 'time': "STRING" }
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


            #include extension only is specified; remove extension in extesnsion; remove "skip" types
            if path.startswith("Extension") and not any(x.find(fullPath) > -1 for x in config['includeExtensions']):
                continue
            if 'inExtension' in config and config['inExtension']==True and definition['type'] =='Extension':
                continue
            if definition['type'] in skip:
                continue


            if name.upper() in reserved_words:
                    name = '`' + name + '`'
                    
            if definition['type'] in typeMapping:
                sqlType = typeMapping[definition['type']]
                
                if 'isArray' in definition and definition['isArray']==True:
                    addschema = name + colon + "array<{0}>,".format(sqlType)
                else:
                    addschema = name + colon + "{0},".format(sqlType)
                schema = schema + addschema

    
            elif definition['type'] in ["BackboneElement", "Element"]:
                fields = AWSbuildSchema(path, definitions, config, elemFullPath, recursionList_copy, ':')
                
                if 'isArray' in definition and definition['isArray']==True:
                	addschema = "{0}{1} array<struct<{2}>>,".format(name,colon,fields)
                else:
                	addschema = "{0}{1} struct<{2}>,".format(name,colon,fields)
                schema = schema + addschema
                    
            elif definition['type'] == 'Extension':
                newConfig ={**config, 'defaultRecursionLimit': 1}
                if 'inExtension' in config and config['inExtension'] ==True:
                    newConfig.update({'inExtension':True})

                fields = AWSbuildSchema(definition['type'], definitions, newConfig, elemFullPath, recursionList_copy, ':')
                if len(fields) >0:
                    
                    if 'isArray' in definition and definition['isArray']==True:
                        addschema = "{0}{1} array<struct<{2}, parent:STRING, url:STRING>>,".format(name,colon,fields)
                    else:
                    	addschema = "{0}{1} struct<{2}, parent:STRING, url:STRING>,".format(name,colon,fields)
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

                fields = AWSbuildSchema(definition['type'], definitions, newConfig, elemFullPath, recursionList_copy, ':')

                if len(fields) >0:
                    if 'isArray' in definition and definition['isArray']==True:
                        addschema = "{0}{1} array<struct<{2}>>,".format(name,colon,fields)
                    else:
                        addschema = "{0}{1} struct<{2}>,".format(name,colon,fields)
                    schema = schema + addschema


            # annotation for query performance
            if 'includeAA' in config and config['includeAA'] == True:
                #remove `` from reserved words
                name = name.replace('`', '')
                if definition['type'] in ("date", "dateTime", "instant"):
                    fields = "start:STRING, `end`: STRING"
                    if 'isArray' in definition and definition['isArray']==True:
                        addschema = "{0}_aa{1} array<struct<{2}>>,".format(name,colon,fields)
                    else :
                        addschema = "{0}_aa{1} struct<{2}>,".format(name,colon,fields)
                    schema = schema + addschema
                    
                elif path == "Reference.reference":
                    addschema = "reference_id_aa {0} STRING, reference_prev_aa {1} STRING,".format(colon,colon)
                    schema = schema + addschema
                elif definition['type']== "string" and path != "Extension.parent" and name != "resourceType" and name != "reference":
                    if 'isArray' in definition and definition['isArray']==True:
                        addschema = "{0}_aa{1} array<STRING>,".format(name,colon)
                    else:
                        addschema = "{0}_aa{1} STRING,".format(name,colon)
                    schema = schema + addschema
                elif name =="resourceType":
                    addschema = "id_prev_aa{0} STRING,".format(colon)
                    schema = schema + addschema
    schema = schema[:-1]
    return schema

