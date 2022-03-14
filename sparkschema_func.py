#!/usr/bin/env python3
# -*- coding: utf-8 -*-




from collections import OrderedDict
from annotation_func import buildBundle
import re

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

def sparkBuildSchema(basePath, definitions, config={}, fullPath='', recursionList={}):
    if 'includeExtensions' in  config:
        new_include_ext = []
        for i in config['includeExtensions']:
            i = ext_flat_trim(i)
            new_include_ext += [i]
        config['includeExtensions'] = new_include_ext

    schema=''
    typeMapping = { 'boolean': "BooleanType()", 
                    'integer': "IntegerType()", 
                    'decimal': "DoubleType()", 
                    'unsignedInt': "IntegerType()", 
                    'positiveInt': "IntegerType()",
		            'fhirid': "StringType()", 
                    'string': "StringType()", 
                    'code': "StringType()", 
                    'uri': "StringType()", 
                    'url': "StringType()", 
                    'canonical': "StringType()",
                    'base64Binary': "StringType()", 
                    'oid': "StringType()", 
                    'id': "StringType()", 
                    'uuid': "StringType()", 
                    'markdown': "StringType()",
                    'instant': "StringType()", 
                    'date': "StringType()", 
                    'dateTime': "StringType()", 
                    'time': "StringType()" }
    skip = ["Narrative", "markdown", "Resource"] 

    #reserved_words = ["ALL", "ALTER", "AND", "ARRAY", "AS", "AUTHORIZATION", "BETWEEN", "BIGINT", "BINARY", "BOOLEAN", "BOTH", "BY", "CASE", "CASHE", "CAST", "CHAR", "COLUMN", "CONF", "CONSTRAINT", "COMMIT", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURSOR", "DATABASE", "DATE", "DAYOFWEEK", "DECIMAL", "DELETE", "DESCRIBE", "DISTINCT", "DOUBLE", "DROP", "ELSE", "END", "EXCHANGE", "EXISTS", "EXTENDED", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FLOAT", "FLOOR", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "FUNCTION", "GRANT", "GROUP", "GROUPING", "HAVING", "IF", "IMPORT", "IN", "INNER", "INSERT", "INT", "INTEGER,INTERSECT", "INTERVAL", "INTO", "IS", "JOIN", "LATERAL", "LEFT", "LESS", "LIKE", "LOCAL", "MACRO", "MAP", "MORE", "NONE", "NOT", "NULL", "NUMERIC", "OF", "ON", "ONLY", "OR", "ORDER", "OUT", "OUTER", "OVER", "PARTIALSCAN", "PARTITION", "PERCENT", "PRECEDING", "PRECISION", "PRESERVE", "PRIMARY", "PROCEDURE", "RANGE", "READS", "REDUCE", "REGEXP,REFERENCES", "REVOKE", "RIGHT", "RLIKE", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "SELECT", "SET", "SMALLINT", "START,TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP", "TO", "TRANSFORM", "TRIGGER", "TRUE", "TRUNCATE", "UNBOUNDED,UNION", "UNIQUEJOIN", "UPDATE", "USER", "USING", "UTC_TIMESTAMP", "VALUES", "VARCHAR", "VIEWS", "WHEN", "WHERE", "WINDOW", "WITH"]
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
            if path.startswith("Extension") and not any(x.find(elemFullPath) > -1 for x in config['includeExtensions']):
                continue
            #if 'inExtension' in config and config['inExtension']==True and definition['type'] =='Extension':
            #    continue
            if definition['type'] in skip:
                continue


            #if name.upper() in reserved_words:
            #        name = '`' + name + '`'
                    
            if definition['type'] in typeMapping:
                sqlType = typeMapping[definition['type']]
                
                if 'isArray' in definition and definition['isArray']==True:
                    #addschema = name + colon + "array<{0}>,".format(sqlType)
                    addschema = "StructField(\"{0}\",ArrayType({1})),".format(name,sqlType)

                else:
                    #addschema = name + colon + "{0},".format(sqlType)
                    addschema = "StructField(\"{0}\",{1}),".format(name,sqlType)
                schema = schema + addschema

    
            elif definition['type'] in ["BackboneElement", "Element"]:
                fields = sparkBuildSchema(path, definitions, config, elemFullPath, recursionList_copy)
                
                if 'isArray' in definition and definition['isArray']==True:
                	#addschema = "{0}{1} array<struct<{2}>>,".format(name,colon,fields)
                    addschema = "StructField(\"{0}\",ArrayType(StructType([{1}]))),".format(name,fields)
                else:
                	#addschema = "{0}{1} struct<{2}>,".format(name,colon,fields)
                    addschema = "StructField(\"{0}\",StructType([{1}])),".format(name,fields)
                schema = schema + addschema
                    
            elif definition['type'] == 'Extension':
                newConfig ={**config, 'defaultRecursionLimit': 1}
                if 'inExtension' in config and config['inExtension'] ==True:
                    newConfig.update({'inExtension':True})

                fields = sparkBuildSchema(definition['type'], definitions, newConfig, elemFullPath, recursionList_copy)
                if len(fields) >0:
                    
                    if 'isArray' in definition and definition['isArray']==True:
                        #addschema = "{0}{1} array<struct<{2}, parent:STRING, url:STRING>>,".format(name,colon,fields)
                        addschema = "StructField(\"{0}\",ArrayType(StructType([{1},StructField(\"parent\",StringType()),StructField(\"url\",StringType())]))),".format(name,fields)
                    else:
                    	#addschema = "{0}{1} struct<{2}, parent:STRING, url:STRING>,".format(name,colon,fields)
                        addschema = "StructField(\"{0}\",StructType([{1},StructField(\"parent\",StringType()),StructField(\"url\",StringType())])),".format(name,fields)
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

                fields = sparkBuildSchema(definition['type'], definitions, newConfig, elemFullPath, recursionList_copy)

                if len(fields) >0:
                    if 'isArray' in definition and definition['isArray']==True:
                        #addschema = "{0}{1} array<struct<{2}>>,".format(name,colon,fields)
                        addschema = "StructField(\"{0}\",ArrayType(StructType([{1}]))),".format(name,fields)
                    else:
                        #addschema = "{0}{1} struct<{2}>,".format(name,colon,fields)
                        addschema = "StructField(\"{0}\",StructType([{1}])),".format(name,fields)
                    schema = schema + addschema


            # annotation for query performance
            if 'includeAA' in config and config['includeAA'] == True:
                #remove `` from reserved words
                #name = name.replace('`', '')
                if definition['type'] in ("date", "dateTime", "instant"):
                    fields = "StructField(\"start\",StringType()),StructField(\"end\",StringType()),"
                    if 'isArray' in definition and definition['isArray']==True:
                        #addschema = "{0}_aa{1} array<struct<{2}>>,".format(name,colon,fields)
                        addschema = "StructField(\"{0}_aa\",ArrayType(StructType([{1}]))),".format(name,fields)
                    else :
                        #addschema = "{0}_aa{1} struct<{2}>,".format(name,colon,fields)
                        addschema = "StructField(\"{0}_aa\",StructType([{1}])),".format(name,fields)
                    schema = schema + addschema
                    
                elif path == "Reference.reference":
                    #addschema = "reference_id_aa {0} STRING, reference_prev_aa {1} STRING,".format(colon,colon)
                    addschema = "StructField(\"reference_id_aa\",StringType()),StructField(\"reference_prev_aa\",StringType()),"
                    schema = schema + addschema
                elif definition['type']== "string" and path != "Extension.parent" and name != "resourceType" and name != "reference":
                    if 'isArray' in definition and definition['isArray']==True:
                        #addschema = "{0}_aa{1} array<STRING>,".format(name,colon)
                        addschema = "StructField(\"{0}_aa\",ArrayType(StringType())),".format(name)
                    else:
                        #addschema = "{0}_aa{1} STRING,".format(name,colon)
                        addschema = "StructField(\"{0}_aa\",StringType()),".format(name)
                    schema = schema + addschema
                elif name =="resourceType":
                    #addschema = "id_prev_aa{0} STRING,".format(colon)
                    addschema = "StructField(\"id_prev_aa\",StringType()),".format(name)
                    schema = schema + addschema
    schema = schema[:-1]
    return schema


















