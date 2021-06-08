#schema test


import unittest
import re
from schema_func import AWSbuildSchema


###### Athena schema output is in a loooong string. The string might contain multiple space to mess up the test.

class Test_awsschema(unittest.TestCase):
	def test_schema(self):
		
		#simple types
		definitions = {
			"Patient.gender": {"type": "code"}
		}
		self.assertEqual(AWSbuildSchema("Patient", definitions), "gender STRING")


		#repeating simple types, yeah, this isn't valid FHIR
		definitions = {
			"Patient.gender": {"type": "code", "isArray": True}
		}
		self.assertEqual(AWSbuildSchema("Patient", definitions), "gender array<STRING>")


		#backbone element. yeah, this isn't valid FHIR
		definitions = {
			"Patient.contact": {"type": "BackboneElement", "isArray": True},
			"Patient.contact.code": {"type": "code"}
		} 
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Patient", definitions)) , "contact array<struct<code:STRING>>")


		#complex types
		definitions = {
			"Patient.name": {"type": "HumanName", "isArray": True},
			"HumanName.given": {"type": "string", "isArray": True},
			"HumanName.use": {"type": "code"},
			"HumanName.period": {"type": "Period"},
			"Period.start": {"type": "dateTime"},
			"Period.end": {"type": "dateTime"}
		}
		#print(AWSbuildSchema("Patient", definitions))
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Patient", definitions)) , "name array<struct<given:array<STRING>,period: struct<`end`:STRING,start:STRING>,use:STRING>>")


		#limit recursive elements (contextReferences)
		definitions = {
			"Questionnaire.item": {"type": "BackboneElement", "isArray": True},
			"Questionnaire.item.item": {"type": "ContentReference", "contentReference": "Questionnaire.item"},
			"Questionnaire.item.id": {"type": "fhirid"}
		}
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Questionnaire", definitions,config={})), "item array<struct<id:STRING,item: array<struct<id:STRING,item: array<struct<id:STRING>>>>>>")



		#limit recursive elements by path (contextReferences)
		definitions = {
			"Questionnaire.item": {"type": "BackboneElement", "isArray": True},
			"Questionnaire.item.item": {"type": "ContentReference", "contentReference": "Questionnaire.item"},
			"Questionnaire.item.id": {"type": "fhirid"}
		}
		recursionLimits = {"Questionnaire.item": 2}
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Questionnaire", definitions, config={"recursionLimits":recursionLimits})), "item array<struct<id:STRING,item: array<struct<id:STRING>>>>")
		

		
		#limit recursive elements (complex types)
		# cannot handle structure without contents - "Identifier.gender": {"type": "code"}
		# Identifier.assigner not showing in 3rd time -- explanation patient.Reference repeated 3 times, 
		definitions = {
			"Patient.managingOrganization": {"type": "Reference"},
			"Reference.identifier": {"type": "Identifier"},
			"Identifier.assigner": {"type": "Reference"},
			"Identifier.gender": {"type": "code"}
		}
		#print(AWSbuildSchema("Patient", definitions))
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Patient", definitions)), "managingOrganization struct<identifier: struct<assigner: struct<identifier: struct<assigner: struct<identifier: struct<gender:STRING>>,gender:STRING>>,gender:STRING>>")



		#limit recursive element by path (complex types)
		definitions = {
			"Patient.managingOrganization": {"type": "Reference"},
			"Reference.identifier": {"type": "Identifier"},
			"Identifier.assigner": {"type": "Reference"},
			"Identifier.gender": {"type": "code"}
		}
		recursionLimits = {"Identifier.assigner": 1}
		#print(AWSbuildSchema("Patient", definitions, config={"recursionLimits":recursionLimits}))
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Patient", definitions, config={"recursionLimits":recursionLimits})), "managingOrganization struct<identifier: struct<assigner: struct<identifier: struct<gender:STRING>>,gender:STRING>>")



		#don't add extensions to elements in extensions
		definitions = {
			"Patient.extension": {"type": "Extension"},
			"Extension.valueHumanName": {"type": "HumanName"},
			"HumanName.family": {"type": "string"},
			"HumanName.extension": {"type": "Extension"}
		}
		includeExtensions = ["Patient.extension.valueHumanName"]
		#print(AWSbuildSchema("Patient", definitions, config={"includeExtensions":includeExtensions}))
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Patient", definitions, config={"includeExtensions":includeExtensions})), "extension struct<valueHumanName: struct<family:STRING>, parent:STRING, url:STRING>")



		###### only include specified extension elements


		# don't include extension without a value
		definitions = {
			"Patient.name": {"type": "HumanName"},
			"HumanName.family": {"type": "string"},
			"HumanName.extension": {"type": "Extension"},
			"Extension.valueString": {"type": "string"},
			"Extension.url": {"type": "string"},
			"Extension.extension": {"type": "Extension"}
		}
		#print(AWSbuildSchema("Patient", definitions, config={"includeExtensions":[], "includeAA":True}))
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Patient", definitions, config={"includeExtensions":[], "includeAA":True})), "name struct<family:STRING,family_aa: STRING>")


		# skip non-analytical elements
		definitions = {
			"Patient.gender": {"type": "code"},
			"Patient.text": {"type": "Narrative"}
		}
		#print(AWSbuildSchema("Patient", definitions))
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Patient", definitions)) , "gender STRING")


		######augment Extension
		#url:STRING showed up twice due to resolving all node in "Extension" in the new setup
		definitions = {
			"Patient.extension": {"type": "Extension", "isArray": True},
			"Extension.extension": {"type": "Extension", "isArray": True},
			"Extension.url": {"type": "url"},
			"Extension.valueString": {"type": "string"}
		}
		includeExtensions = ["Patient.extension.valueString"]
		#print(AWSbuildSchema("Patient", definitions,  config={"includeAA": True, "includeExtensions":includeExtensions}))
		#extension  array<struct<url:STRING,valueString:STRING,valueString_aa: STRING, parent:STRING, url:STRING>>



		#augment Reference
		definitions = {
			"Patient.managingOrganization": {"type": "Reference"},
			"Reference.reference": {"type": "string"},
			"Reference.display": {"type": "string"},
		}
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Patient", definitions, {"includeAA": True})) , "managingOrganization struct<display:STRING,display_aa: STRING,reference:STRING,reference_id_aa : STRING, reference_prev_aa : STRING>")


		#augment dateTime
		definitions = {
			"Patient.birthDate": {"type": "dateTime"}
		}
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Patient", definitions, {"includeAA": True})) , "birthDate STRING,birthDate_aa struct<start:STRING, `end`: STRING>")


		#augment string //yeah, this isn't valid FHIR
		definitions = {
			"Patient.comment": {"type": "string"}
		}
		#print(AWSbuildSchema("Patient", definitions, {"includeAA": True}))
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Patient", definitions, {"includeAA": True})) , "comment STRING,comment_aa STRING")


		#augment base resource //yeah, this isn't valid FHIR
		definitions = {
			"Patient.resourceType": {"type": "string"}
		}
		self.assertEqual(re.sub(r'\s+', ' ', AWSbuildSchema("Patient", definitions, {"includeAA": True})) , "resourceType STRING,id_prev_aa STRING")



if __name__ == '__main__':
	unittest.main()