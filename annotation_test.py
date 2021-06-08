#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# test cases for ETL process
import unittest
from annotation_func import *



class Test_ETL(unittest.TestCase):

	
	#dates to range
	def test_transformDate(self):
		self.assertEqual(transformDate('2019')                   , {"start":'2019-01-01T00:00:00.000Z',"end": '2019-12-31T23:59:59.999Z'})
		self.assertEqual(transformDate('2019-02')                , {"start":'2019-02-01T00:00:00.000Z',"end": '2019-02-28T23:59:59.999Z'})
		self.assertEqual(transformDate('2019-02-03')             , {"start":'2019-02-03T00:00:00.000Z',"end": '2019-02-03T23:59:59.999Z'})     
		self.assertEqual(transformDate('2019-02-03T03:22:00')    , {"start":'2019-02-03T03:22:00.000Z',"end": '2019-02-03T03:22:00.999Z'})
		self.assertEqual(transformDate('2019-02-03T03:22:00.00Z'), {"start":'2019-02-03T03:22:00.000Z',"end": '2019-02-03T03:22:00.009Z'})
		#test "inject tz offset if passed in"
		self.assertEqual(transformDate("2019-02", "-5:00")		 , {"start":"2019-02-01T00:00:00.000-5:00","end": "2019-02-28T23:59:59.999-5:00"})

		self.assertRaises(ValueError, transformDate,'2019-13')
		self.assertRaises(ValueError, transformDate,'2019-1')
		self.assertRaises(ValueError, transformDate,' 2019-01')
		self.assertRaises(ValueError, transformDate,'2019-')
		self.assertRaises(ValueError, transformDate,'2019-01-29T24:01:00Z')
		return
	
	def test_transformReference(self):
		definitions = {
		"Reference.reference": {"type": "string"},
		"Reference.type": {"type": "uri"},
		"Reference.display": {"type": "string"}
	}
		#convert a full url
		self.assertEqual(transformReference({"reference": "http://example.com/fhir/Patient/123"}, "", [], definitions), 
			{
			"reference": "Patient/6ac3526602cd8620ab764c2781522cdbf2db276d3472b19fe4dd67bdb9057344",
			"reference_id_aa": "6ac3526602cd8620ab764c2781522cdbf2db276d3472b19fe4dd67bdb9057344",
			"reference_prev_aa": "http://example.com/fhir/Patient/123",
			"type": "Patient"
			},"convert a full url")
		
		#ignore protocol
		self.assertEqual(transformReference({"reference": "https://example.com/fhir/Patient/123"}, "", [], definitions)["reference_id_aa"],
						 transformReference({"reference": "http://example.com/fhir/Patient/123"}, "", [], definitions)["reference_id_aa"],
						 "ignore protocol")
		
		#convert a relative url
		self.assertEqual(transformReference({"reference": "Patient/123"}, "http://example.com/fhir/", [], definitions),
			{
			"reference": "Patient/6ac3526602cd8620ab764c2781522cdbf2db276d3472b19fe4dd67bdb9057344",
			"reference_id_aa": "6ac3526602cd8620ab764c2781522cdbf2db276d3472b19fe4dd67bdb9057344",
			"reference_prev_aa": "Patient/123",
			"type": "Patient"
		},"convert a relative url ")


		
		#convert a contained url
		containedResources = [{
			"id_prev_aa": "123#p1", 
			"resourceType": "Patient", 
			"id": "6521715c6076989744a0cbedc0133341054f0264"
		}]
		self.assertEqual(transformReference({"reference": "#p1"}, "http://example.com/fhir/", containedResources, definitions), \
			{
			"reference": "Patient/6521715c6076989744a0cbedc0133341054f0264",
			"reference_id_aa": "6521715c6076989744a0cbedc0133341054f0264",
			"reference_prev_aa": "#p1",
			"type": "Patient"
		})
			
	
		
		#retain non-url elements
		self.assertEqual(transformReference({"reference": "http://example.com/fhir/Patient/123","display": "test"}, "", [], definitions),{
			"reference": "Patient/6ac3526602cd8620ab764c2781522cdbf2db276d3472b19fe4dd67bdb9057344",
			"reference_id_aa": "6ac3526602cd8620ab764c2781522cdbf2db276d3472b19fe4dd67bdb9057344",
			"reference_prev_aa": "http://example.com/fhir/Patient/123",
			"type": "Patient",
			"display": "test",
			"display_aa": "TEST"
		})
		
	def test_text(self):
		#capitalize and remove diacritics
		self.assertEqual(transformText("Crème Brulée"),"CREME BRULEE")
		
		
	def test_extensions(self):
		
		definitions = {"Extension.valueCode": {"type": "code"}}
		extension = [{"url": "http://hl7.org/fhir/StructureDefinition/iso-21090-EN-use","valueCode": "I"}]
		includeExtensions = ["Extension.valueCode"]
		

		#promote extensions
		self.assertEqual(transformExtension(extension, definitions, {"includeExtensions":includeExtensions}, ""),
			[{
			"url": "http://hl7.org/fhir/StructureDefinition/iso-21090-EN-use",
			"valueCode": "I",
			"parent": "",
		}])
		
		#don't support extensions on a complex type in an extension
		definitions = {
			"Extension.valueHumanName": {"type": "HumanName"},
			"HumanName.given": {"type": "string"},
			"HumanName.extension": {"type": "Extension"},
		}
		extension = [{
			"url": "url1",
			"valueHumanName": { 
				"given": "Dan",
				"extension": [{
					"url": "url2",
					"valueHumanName": { "given": "Dan2" },
				}]
			}
		}]
		includeExtensions = ["Extension.valueHumanName"]
		self.assertEqual(transformExtension(extension, definitions, {"includeExtensions": includeExtensions, "suppressExtensionErrors": True}),
			[{
			"url": "url1",
			"valueHumanName": { "given": "Dan", "given_aa": "DAN"},
			"parent": ""
		}])

		self.assertRaises(Exception, transformExtension , extension=extension, definitions=definitions, schemaConfig = {"includeExtensions": includeExtensions})


		
		#promote nested extensions
		definitions = {
			"Extension.valueCoding": {"type": "Coding"},
			"Extension.valueString": {"type": "string"},
			"Extension.valueCode": {"type": "code"},
			"Extension.url": {"type": "fhirid"},
			"Coding.code": {"type": "code"}
		}
		includeExtensions = ["Extension.valueCoding", "Extension.valueCode", "Extension.valueString"]
		extension = [{
			"url" : "http://fhir.org/guides/argonaut/StructureDefinition/argo-ethnicity",
			"extension" : [{
				"url" : "ombCategory",
				"valueCoding" : {"code" : "2135-2"}
			},{
				"url" : "ombCategory",
				"valueCoding" : {"code" : "1002-5"}
			},{
				"url" : "text",
				"valueString" : "Hispanic"
			}]
		},{
			"url" : "http://fhir.org/guides/argonaut/StructureDefinition/argo-birthsex",
			"valueCode" : "M"
		}]
		self.assertEqual(transformExtension(extension, definitions, {"includeExtensions":includeExtensions}),
			[{
			"url": "http://fhir.org/guides/argonaut/StructureDefinition/argo-ethnicity/ombCategory",
			"valueCoding": { "code": '2135-2' },
			"parent": "0"
		},{
			"url": 'http://fhir.org/guides/argonaut/StructureDefinition/argo-ethnicity/ombCategory',
			"valueCoding": { "code": '1002-5' },
			"parent": "0"
		},{
			"parent": "0",
			"url": "http://fhir.org/guides/argonaut/StructureDefinition/argo-ethnicity/text",
			"valueString_aa": "HISPANIC",
			"valueString": "Hispanic"
		},{
			"url": "http://fhir.org/guides/argonaut/StructureDefinition/argo-birthsex",
			"valueCode": "M",
			"parent": ""
		}])

		
		
	def test_resource(self):

		#augment simple type
		resource = {"id": "1234","resourceType": "Patient","birthDate": "2020-01-01"}
		definitions = {"Patient.id": {"type": "fhirid"},"Patient.birthDate": {"type": "date"}}
		self.assertEqual(transformResource(resource, definitions),{
			"id": '7cb22834bdd4c55291d65e30f5d84a12b45369b8d1d71c7726ecb8cac2ac7d42',
			"id_prev_aa": "1234",
			"resourceType": 'Patient',
			"birthDate": "2020-01-01",
			"birthDate_aa": {
				"start": "2020-01-01T00:00:00.000Z",
				"end": "2020-01-01T23:59:59.999Z"
			}
		})

		#fail on missing definition
		resource = {"id": "1234","resourceType": "Patient","birthDate": "2020-01-01"}
		definitions = {"Patient.id": {"type": "fhirid"}}
		self.assertRaises(TypeError, transformResource , resource=resource, definitions=definitions)

		
		#augment array of simple types
		resource = {"id": "1234","resourceType": "Patient","name": [{"given": ["Patricia"]}]}
		definitions = {"Patient.id": {"type": "fhirid"},"Patient.name": {"type": "HumanName"},"HumanName.given": {"type": "string"},}
		self.assertEqual(transformResource(resource, definitions), {
			"id": '7cb22834bdd4c55291d65e30f5d84a12b45369b8d1d71c7726ecb8cac2ac7d42',
			"id_prev_aa": "1234",
			"resourceType": 'Patient',
			"name": [{
				"given": ["Patricia"],
				"given_aa": ["PATRICIA"]
			}]
		})

		#augment array of complex types
		resource = {
			"id": "1234",
			"resourceType": "Patient",
			"name": [{
				"use": "official",
				"given": ["Patricia"]
			},{
				"use": "nickname",
				"given": ["Pat"]
			}]
		}
		definitions = {
			"Patient.id": {"type": "fhirid"},
			"Patient.name": {"type": "HumanName"},
			"HumanName.given": {"type": "string"},
			"HumanName.use": {"type": "code"}
		}
		self.assertEqual(transformResource(resource, definitions),{
			"id": '7cb22834bdd4c55291d65e30f5d84a12b45369b8d1d71c7726ecb8cac2ac7d42',
			"id_prev_aa": "1234",
			"resourceType": 'Patient',
			"name": [{
				"use": "official",
				"given": ["Patricia"],
				"given_aa": ["PATRICIA"]
			},{
				"use": "nickname",
				"given": ["Pat"],
				"given_aa": ["PAT"]
			}]
		})


		######augment contained resource
		#contained resource is not beign handled in the function due to multiple file output requirement
		#augment contained resource that references parent resource
		
		
		#augment extension
		resource = {
			"id": "1234",
			"resourceType": "Patient",
			"extension": [{
				"url" : "http://fhir.org/guides/argonaut/StructureDefinition/argo-ethnicity",
				"extension" : [{
					"url" : "ombCategory",
					"valueCoding" : {"code" : "2135-2"}
				},{
					"url" : "ombCategory",
					"valueCoding" : {"code" : "1002-5"}
				}]
			}]
		}
		definitions = {
			"Patient.extension": {"type": "Extension"},
			"Extension.valueCoding": {"type": "Coding"},
			"Coding.code": {"type": "code"},
			"Patient.id": {"type": "fhirid"}
		}
		includeExtensions = ["Patient.extension.valueCoding"]
		#includeExtensions = ["Patient.extension"]

		self.assertEqual(transformResource(resource, definitions, "", "", {"includeExtensions":includeExtensions}),
			{
			"id": '7cb22834bdd4c55291d65e30f5d84a12b45369b8d1d71c7726ecb8cac2ac7d42',
			"id_prev_aa": "1234",
			"resourceType": 'Patient',
			"extension": [{
				"url": "http://fhir.org/guides/argonaut/StructureDefinition/argo-ethnicity/ombCategory",
				"valueCoding": { "code": '2135-2' },
				"parent": '0'
			},{
				"url": 'http://fhir.org/guides/argonaut/StructureDefinition/argo-ethnicity/ombCategory',
				"valueCoding": { "code": '1002-5' },
				"parent": '0' 
			}]
		})

		"""
		######Do we need to keep this function? or be consistent with schema gen, to include everthing in first layer of the extension
		#limit extensions to defined elements
		resource = {
			"id": "1234",
			"resourceType": "Patient",
			"extension": [{
				"url" : "http://fhir.org/guides/argonaut/StructureDefinition/argo-ethnicity",
				"extension" : [{
					"url" : "ombCategory",
					"valueCoding" : {"code" : "2135-2"}
				},{
					"url" : "ombCategory",
					"valueString" : "text"
				}]
			}]
		}
		definitions = {
			"Patient.extension": {"type": "Extension"},
			"Extension.valueCoding": {"type": "Coding"},
			"Extension.valueString": {"type": "string"},
			"Coding.code": {"type": "code"},
			"Patient.id": {"type": "fhirid"}
		}
		includeExtensions = ["Patient.extension.valueCoding"]
		print(transformResource(resource, definitions, "", "", {"includeExtensions":includeExtensions,"suppressExtensionErrors":True}))
		self.assertEqual(transformResource(resource, definitions, "", "", {"includeExtensions":includeExtensions,"suppressExtensionErrors":True}),
			{
			"id": '7cb22834bdd4c55291d65e30f5d84a12b45369b8d1d71c7726ecb8cac2ac7d42',
			"id_prev_aa": "1234",
			"resourceType": 'Patient',
			"extension": [{
				"url": "http://fhir.org/guides/argonaut/StructureDefinition/argo-ethnicity/ombCategory",
				"valueCoding": { "code": '2135-2' },
				"parent": '0'
			}]
		})
		"""

		#augment Backbone Elements
		resource = {
			"resourceType": "Questionnaire",
			"item": [{"id": "0"}]	
		}
		definitions = {
			"Questionnaire.item": {"type": "BackboneElement", "isArray": True},
			"Questionnaire.item.id": {"type": "fhirid"}
		}
		self.assertEqual(transformResource(resource, definitions),{
			"resourceType": "Questionnaire",
			"item": [{"id": "0"}]
		})



		#limit recursive elements (contentReference)
		resource = {
			"resourceType": "Questionnaire",
			"item": [{
				"id": "0", "item": [{
					"id": "0.0", "item": [{
						"id": "0.0.0", "item": [{
							"id": "0.0.0.0", "item": [{
								"id": "0.0.0.0.0"
							}]
						}]
					}]
				}]
			}]
		}
		definitions = {
			"Questionnaire.item": {"type": "BackboneElement", "isArray": True},
			"Questionnaire.item.item": {"type": "ContentReference", "contentReference": "Questionnaire.item"},
			"Questionnaire.item.id": {"type": "fhirid"}
		}
		self.assertEqual(transformResource(resource, definitions, "", "", {"defaultRecursionLimit": 2, "suppressRecursionErrors": True}),
			{
			"resourceType": "Questionnaire",
			"item": [{
				"id": "0", "item": [{
					"id": "0.0"
				}]
			}]
		})
		# raise exception when over recursion limit
		self.assertRaises(Exception, transformResource , resource=resource, definitions=definitions)


		#limit recursive elements (complex types)
		resource = {
			"resourceType": "Patient",
			"managingOrganization": {
				"identifier": {
					"value": "0", "assigner": {
						"identifier": {
							"value": "0.0", "assigner": {
								"identifier": {
									"value": "0.0.0", "assigner": {
										"identifier": {"value": "0.0.0.0"}
									}
								}
							}
						}
					}
				}
			}
		}
		definitions = {
			"Patient.managingOrganization": {"type": "Reference"},
			"Reference.identifier": {"type": "Identifier"},
			"Identifier.assigner": {"type": "Reference"},
			"Identifier.value": {"type": "string"}
		}
		self.assertEqual(transformResource(resource, definitions, "", "", {"suppressRecursionErrors": True}),
			{
			"resourceType": "Patient",
			"managingOrganization": {
				"identifier": {
					"value": "0", "value_aa": "0", "assigner": {
						"identifier": {
							"value": "0.0", "value_aa": "0.0", "assigner": {
								"identifier": {
									"value": "0.0.0", "value_aa":"0.0.0", 
									"assigner": {}
								}
							}
						}
					}
				}
			}
		})
		#fail if error on recursion is true
		#self.assertRaises(SystemExit, transformResource , value=resource, definitions=definitions)
		print(transformResource(resource, definitions))	



		
if __name__ == '__main__':
	unittest.main()