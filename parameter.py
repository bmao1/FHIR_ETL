#!/usr/bin/env python3
# -*- coding: utf-8 -*-



extensions=['Patient.extension.extension.valueCoding',
 'Patient.extension.extension.valueString',
 'Patient.extension.valueCode',
 'MedicationStatement.medicationCodeableConcept.extension.extension.valueInteger',
 'MedicationStatement.modifierExtension.valueInteger',
 'MedicationStatement.modifierExtension.extension.valueCodeableConcept.coding',
 'MedicationStatement.modifierExtension.extension.valueCodeableConcept.text',
 'MedicationStatement.modifierExtension.extension.valueCodeableConcept',
 'MedicationStatement.modifierExtension.extension.valueDate',
 'MedicationStatement.modifierExtension.extension.valueString',
 'Observation.code.extension.extension.valueInteger',
 'Observation.modifierExtension.valueInteger',
 'Observation.modifierExtension.extension.valueCodeableConcept.coding',
 'Observation.modifierExtension.extension.valueCodeableConcept.text',
 'Observation.modifierExtension.extension.valueCodeableConcept',
 'Observation.modifierExtension.extension.valueDate',
 'Observation.modifierExtension.extension.valueString',
 'Observation.modifierExtension.valueString',
 'Condition.code.extension.extension.valueInteger',
 'Condition.modifierExtension.valueInteger',
 'Condition.modifierExtension.extension.valueCodeableConcept.coding',
 'Condition.modifierExtension.extension.valueCodeableConcept.text',
 'Condition.modifierExtension.extension.valueCodeableConcept',
 'Condition.modifierExtension.extension.valueDate',
 'Condition.modifierExtension.extension.valueString',
 'Procedure.code.extension.extension.valueInteger',
 'Procedure.modifierExtension.valueInteger',
 'Procedure.modifierExtension.extension.valueCodeableConcept.coding',
 'Procedure.modifierExtension.extension.valueCodeableConcept.text',
 'Procedure.modifierExtension.extension.valueCodeableConcept',
 'Procedure.modifierExtension.extension.valueDate',
 'Procedure.modifierExtension.extension.valueString']
recursionLimits = {"Identifier.assigner": 1}
#recursionLimits = {}
config = {
'includeExtensions': extensions,
'recursionLimits': recursionLimits,
'includeAA': True
}



