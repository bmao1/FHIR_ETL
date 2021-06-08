#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#folder_loc= "/home/ubuntu/code/a3/etl"
folder_loc= "/Users/binmao/Documents/bulkdata_analytics/ETL_combined"


extensions=[
		"Patient.extension",
		"Patient.address.extension",

		"MedicationStatement.medicationCodeableConcept.extension.extension",
		"MedicationStatement.modifierExtension.extension",

		"Observation.code.extension.extension",
		"Observation.modifierExtension.extension",

		"Condition.code.extension.extension",
		"Condition.modifierExtension.extension",		

		"Procedure.code.extension.extension",
		"Procedure.modifierExtension.extension"
	]
recursionLimits = {"Identifier.assigner": 1}
#recursionLimits = {}
config = {
		'includeExtensions': extensions, 
		'recursionLimits': recursionLimits,
        'includeAA': True
	}

schema_path = "/Users/binmao/Documents/aws/schema/"
#schema_path = "/home/ubuntu/code/schema/generatedschema/"


