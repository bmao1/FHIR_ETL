#!/usr/bin/env python3
# -*- coding: utf-8 -*-



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



