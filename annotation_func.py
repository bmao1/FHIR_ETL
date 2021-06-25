#!/usr/bin/env python3
# -*- coding: utf-8 -*-



############ trimUrl, remove last / ##############
import re
def trimUrl(url):
    return re.sub(r'\/$', '', url)
    #return url.split("/")[-1]

#trimUrl('/andre/23/abobora/43435/')
#trimUrl('/andre/23/abobora/43435')

############ url encrypt / decrypt ##############
from cryptography.fernet import Fernet
import hashlib
key = Fernet.generate_key()  # store in a secure location
#key ="HpeD2Op4W5NO7TfDuSwPr4asdB8sH5twk47xSDMq6Nc="
#key = key.encode()
print("Key:", key.decode())

def urlToHash(url):
    hashUrl = re.sub("^http(s?)://","",url)
    return hashlib.sha256(hashUrl.encode()).hexdigest()

def EncryptToUrl(EncryptStr):
    return Fernet(key).decrypt(EncryptStr.encode()).decode()

def urlToEncrypt(url):
    EncryptUrl = re.sub("^http(s?)://","",url)
    return Fernet(key).encrypt(EncryptUrl.encode()).decode()

#urlToHash("https://example.com/fhir/")


############ tranform date ##############
#import re
from dateutil.parser import isoparse
from dateutil.tz import UTC
from dateutil.relativedelta import relativedelta
from pytz import timezone, all_timezones

def transformDate(value, tzOffset="UTC"):
    start_dt = isoparse(value)
    if tzOffset in all_timezones:
        start_str = start_dt.replace(tzinfo=timezone(tzOffset)).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    else :
        start_str = start_dt.replace(tzinfo=timezone("UTC")).isoformat(timespec='milliseconds').replace('+00:00', tzOffset)
        
    add_msec =0
    add_date =[0,0,0,0,0]
    # [year, month, date, sec, milsec]
    if re.match("^\d{4}$", value):
        add_date =[1,0,0,0,0]
    elif re.match("^\d{4}-\d{2}$", value):
        add_date =[0,1,0,0,0]
    elif re.match("^\d{4}-\d{2}-\d{2}$", value):
        add_date =[0,0,1,0,0]
    elif re.match("^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", value):
        add_date =[0,0,0,1,0]
    elif re.match("^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.", value):
        #adjust for milsec
        msec_len = len(re.split(r'\D+',value.split(".")[1])[0])
        if msec_len == 1:
            add_date =[0,0,0,0,100]
        elif msec_len == 2:
            add_date =[0,0,0,0,10]
    if max(add_date) >0:
        add_msec = -1 + add_date[4]
        
    day_delta = relativedelta(years=add_date[0], months= add_date[1], days=add_date[2], second=add_date[3], microseconds= add_msec*1000)
    end_dt = start_dt + day_delta
    if tzOffset in all_timezones:
        end_str = end_dt.replace(tzinfo=timezone(tzOffset)).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    else :
        end_str = end_dt.replace(tzinfo=timezone("UTC")).isoformat(timespec='milliseconds').replace('+00:00', tzOffset)
    return {'start':start_str, 'end': end_str}



#transformDate('2019-01-29')    



############ transformReference ##############
def transformReference(value, baseUrl, containedResources, definitions, schemaConfig={}, fullPath="", recursionList={}, parentResource=""):
    recursionList_copy= recursionList.copy()
    value = walkElement(value, definitions, ["Reference"], "", schemaConfig, fullPath, recursionList_copy, [], "" )
    if "reference" not in value:
        return value
    
    if value["reference"][0] == "#":
        try:
            if  value["reference"] != "#":
                for res in containedResources:
                    if res["id_prev_aa"].split("#")[-1] == value["reference"][1:]:
                        resource = res
            else :
                resource = parentResource
            return {**value,
                "reference": "{0}/{1}".format(resource["resourceType"],resource["id"]),
                "reference_id_aa": resource["id"],
                "reference_prev_aa": value["reference"],
                "type": resource["resourceType"]
                }
        except:
            pass

    if value["reference"].startswith("http:") or value["reference"].startswith("https:"):
        url = value["reference"]
    else :
        url = trimUrl(baseUrl) + "/" + value["reference"]
    type = trimUrl(url).split("/")[-2]
    id = urlToHash(url)
    return {**value,
            "reference": "{0}/{1}".format(type,id),
            "reference_id_aa": id,
            "reference_prev_aa": value["reference"],
            "type":type
            }


        





############ transform text ##############
#refer to https://stackoverflow.com/questions/517923/what-is-the-best-way-to-remove-accents-normalize-in-a-python-unicode-string
import unicodedata
def transformText(value):
    return unicodedata.normalize('NFD', value).encode('ascii', 'ignore').decode("utf-8").upper()
    
#transformText("Crème Brulée")



############ transformExtension ##############    


def transformExtension(extension, definitions, schemaConfig, fullPath="", parentUrl="", parentPos=[],flatExtensions=[]):
    for idx, ext in enumerate(extension):
        if len(parentUrl) > 0 :
            parentUrl = trimUrl(parentUrl) + "/"
        else :
            parentUrl = ""
        url = parentUrl + ext["url"]
        ##print("url = {0}".format(url))
        
        pos = []
        if len(parentPos) > 0:
            pos = pos + [p + str(idx) for p in parentPos]
        else :
            pos = pos + [str(idx)]
        ##print("pos = {0}".format(pos))
        if "extension" in ext:
            ##print("To ext url = {0}".format(url))
            flatExtensions  = flatExtensions + transformExtension(ext["extension"], definitions, schemaConfig, "", url, pos, flatExtensions)
        else :
            valueKey = [v for v in ext if ext[v] != "url"]
            #print("valueKey = {0}".format(valueKey))
            if len(parentPos) >0:
                parent = ",".join(parentPos)
            else :
                parent = ""
            newSchemaConfig = {**schemaConfig, "inExtension": True, "defaultRecursionLimit": 1}
            
            value = walkElement({x:ext[x] for x in valueKey if x != "url"}, definitions, ["Extension"], "", newSchemaConfig, fullPath)
            
            if len(value) >0:
                flatExtensions = flatExtensions + [{**value,"url":url, "parent":parent}]

    return flatExtensions


############ transformId ##############
def transformId(value, baseUrl, resourceType):
    url = trimUrl(baseUrl) + "/" + resourceType + "/" + value
    return urlToHash(url)



            
            
############ transformId ##############
def walkElement(element, definitions, path=[], baseUrl="", schemaConfig={}, fullPath="", recursionList={}, containedList=[], parentResource=""):
    output ={}
    for key in element: 
        #print("\n")
        #print("key = {0}".format(key))

        if isinstance(key, list):
            for k in key:
                joinedPathAll = [] + [p + '.' + k for p in path]
            elementValue = [] + [element[k] for k in key]
        else :
            joinedPathAll = [p + '.' + key for p in path]
            #print("joinedPathAll = {0}".format(joinedPathAll))
            elementValue = element[key]
        
        for joinedPath in joinedPathAll:
            recursionList_copy= recursionList.copy()
            #jump to contentReference
            if joinedPath in definitions and definitions[joinedPath]["type"] =="ContentReference":
                joinedPath = definitions[joinedPath]["contentReference"];
        
            if joinedPath in recursionList_copy:       
                recursionList_copy[joinedPath] = recursionList_copy[joinedPath] + 1;
            else :
                recursionList_copy[joinedPath] = 1;

            maxRecursion = 3
            if "recursionLimits" in schemaConfig and joinedPath in schemaConfig["recursionLimits"]:
                maxRecursion = schemaConfig["recursionLimits"][joinedPath]
            elif "defaultRecursionLimit" in schemaConfig:
                maxRecursion = schemaConfig["defaultRecursionLimit"]
        
            if recursionList_copy[joinedPath] > maxRecursion:
                if "suppressRecursionErrors" in schemaConfig and not schemaConfig["suppressRecursionErrors"]:
                    raise Exception("recursionList for {0} over limit {1}".format(joinedPath,maxRecursion))
                    #raise Exception("recursionList over limit ")
                continue
            
            #recursionList_copy= recursionList.copy()
            
            if len(fullPath) >0:
                elemFullPath = fullPath + "." + key
            else :
                elemFullPath = joinedPath
    
            #print("elemFullPath = {0}".format(elemFullPath))
            try:
                if key == "resourceType":
                    definition = {"type": "string"}
                else:
                    definition = definitions[joinedPath]
                #print(definition)
            except:
                print("Definition not found for {0}".format(joinedPath))
                continue
     
            #only allow pre-defined value types in extension
            if path[0]=="Extension" and not any(x.find(fullPath) > -1 for x in schemaConfig['includeExtensions']):
            #if path[0]=="Extension" and not any(x.find(fullPath) > -1 for x in schemaConfig['includeExtensions']):
                #print("Schema not found not found for {0}".format(elemFullPath))
                continue
            
            #don't allow extensions in complex elements in extensions
            if "inExtension" in schemaConfig and schemaConfig["inExtension"] and definition["type"] == "Extension":
                if "suppressExtensionErrors" not in schemaConfig or schemaConfig["suppressExtensionErrors"] != True:
                    print("Nested extension found in {0}".format(elemFullPath))
                    print("right here ++++++++++++++")
                    raise Exception("Exception: Nested extension found in "+ elemFullPath)
                continue
            
            #type is narrative or markdown -> skip
            if 	definition["type"] in ( "Narrative", "markdown","Resource"):
                continue
            
            if definition["type"] == "Extension":
                #print("move to extension with {0}".format(elementValue))
                output[key] = transformExtension(elementValue, definitions, schemaConfig, elemFullPath)
                #print("after extension output: {0}".format(output[key]))
                
            elif definition["type"] == "Reference":
                if isinstance(elementValue, list):
                    output[key] = []
                    for v in elementValue:
                        output[key] = output[key] + [transformReference(v, baseUrl, containedList, definitions, schemaConfig, elemFullPath, {**recursionList_copy}, parentResource)]
                else:
                    output[key] =transformReference(elementValue, baseUrl, containedList, definitions, schemaConfig, elemFullPath, {**recursionList_copy}, parentResource);
    
            #type is a backbone element
            elif definition["type"] in ["BackboneElement", "Element"]:
                #print("elementValue = {0}".format(elementValue))
                if isinstance(elementValue, list):
                    output[key] = []
                    for v in elementValue:
                        output[key] = output[key] + [walkElement(v, definitions, [joinedPath], baseUrl, schemaConfig, elemFullPath, {**recursionList_copy}, containedList, parentResource)]
                else:
                    output[key] = walkElement(elementValue, definitions, [joinedPath], baseUrl, schemaConfig, elemFullPath, {**recursionList_copy}, containedList, parentResource)
		  	
            #type is another complex type
            elif definition["type"][0].isupper():
                #print(definition["type"])
                #print("elementValue = {0}".format(elementValue))
                #print("complex type")
                if isinstance(elementValue, list):
                    output[key] = []
                    for v in elementValue:
                        output[key] = output[key] + [walkElement(v, definitions, [definition["type"]], baseUrl, schemaConfig, elemFullPath, {**recursionList_copy}, containedList, parentResource)]
                else:
                    output[key] = walkElement(elementValue, definitions, [definition["type"]], baseUrl, schemaConfig, elemFullPath, {**recursionList_copy},     containedList, parentResource);

            #type is text
            elif definition["type"] == "string" and key not in ["resourceType", "reference"]:
                #print("elementValue = {0}".format(elementValue))
                if isinstance(elementValue, list):
                    output[key+"_aa"] = [transformText(x) for x in elementValue]
                else:
                    output[key+"_aa"] = transformText(elementValue)
                output[key] = elementValue
                
            #type is date
            elif definition["type"] in ["date", "dateTime", "instant"]:
                if isinstance(elementValue, list):
                    output[key+"_aa"] = [transformDate(x) for x in elementValue]
                else:
                    output[key+"_aa"] = transformDate(elementValue)
                output[key] = elementValue; 
            #truncate decimal precision (bq supports 9 places)   
            elif definition["type"] == "decimal":
                output[key] = round(elementValue,9) 
                
            else:
                output[key] = elementValue
            
    return output
            
            
#value - json input needs to be transfromed
#definitions - structure definition, comes from 
#baseUrl - data source/server to distinguish from other source
#parent - parent node, such as url in the first layer of extension node
#schemaConfig - customize configuration in json format


def transformResource(value, definitions, baseUrl="", parent="", schemaConfig={}):

    if len(parent) > 0 and "id_prev_aa" in parent:
        id_prev_aa = parent["id_prev_aa"] + "#" + value["id"]
    else: 
        try:
            id_prev_aa = value["id"]
        except:
            pass
    if 'id_prev_aa' in locals() and len(id_prev_aa) >0:
        id = transformId(id_prev_aa, baseUrl, value["resourceType"])

    #resourceDetail = {'id':id, 'id_prev_aa':id_prev_aa, 'resourceType': value["resourceType"]}

    #un-contain contained resources
    #if "contained" in value:
    #    for res in value["contained"]:
    #        value["contained"]["res"] = transformResource(res, definitions, baseUrl, resourceDetail, schemaConfig)

    augmented = walkElement(value, definitions, [value["resourceType"]], baseUrl, schemaConfig, "", {}, [], parent)
    try:
        augmented["id_prev_aa"] = id_prev_aa
        augmented["id"] = id
    except:
        pass

    #if len(contained) > 0:
    #    returnValue = contained.concat([augmented])
    #else :
    #    returnValue = augmented
    return augmented



###############################



def buildBundle(infile):
    import json

    with open(infile) as f:
        resources = json.load(f)
    resourceNames = []
    definitions = {}
    for x in resources["entry"]:
        
        if x["resource"]["resourceType"] =='StructureDefinition':
            if x["resource"]["kind"] !='complex-type' and x["resource"]["kind"] != 'resource':
                continue
            if x["resource"]["name"] != x["resource"]["type"]:
                continue
            if x["resource"]["kind"] == 'resource':
                resourceNames = resourceNames + [x["resource"]["name"]]
                definitions[x["resource"]["name"] + ".resourceType"] = {"type": "string"}  
                
            for y in x["resource"]["snapshot"]["element"]:
                if "type" not in y and "contentReference" in y:
                    definitions[y["path"]] = {"type": "ContentReference","isArray": (y["max"] not in ("0", "1") ), "contentReference": str( y["contentReference"])[1:]}
                if "type" in y:
                    for n in y["type"]:
                        if len(y["type"]) ==1:
                            path = y["path"]
                        else :  
                            path = y["path"].replace("[x]", n["code"][:1].upper() + n["code"][1:])  ###########review here
                    
                        if n["code"] =="http://hl7.org/fhirpath/System.String":
                            outputType = "fhirid"
                        else:
                            outputType = n["code"]
                        
                        isArray = (y["max"] != "1" and y["max"] != "0")
                        #referenceTargets = y["type"][0]["targetProfile"].split('/')[-1]
                    
                        if "targetProfile" in n:
                            referenceTargets = []
                            for reft in n["targetProfile"]:
                                referenceTargets = referenceTargets + [reft.split('/')[-1]]
                            definitions[path]={"type": outputType, "isArray":isArray, "referenceTargets":referenceTargets}
                        else :
                            definitions[path]={"type": outputType, "isArray":isArray}
                            

    return {"definitions":definitions, "resourceNames":resourceNames}

