

Instance: PractitionerExample
InstanceOf: Practitioner
Description: "An example representation of a Practitioner"
* meta.profile = "http://hl7.org/fhir/StructureDefinition/Practitioner"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-practitioner-id"
* id = "example-practitioner-id"


Instance: OrganizationExample
InstanceOf: Organization
Description: "An example representation of a Organization"
* meta.profile = "http://hl7.org/fhir/StructureDefinition/Organization"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-organization-id"
* id = "example-organization-id"
* name = "Example Medical School"

Instance: PractitionerRoleExample
InstanceOf: PractitionerRole
Description: "An example representation of a PrincipalInvestigator"
* meta.profile = "http://hl7.org/fhir/StructureDefinition/PractitionerRole"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-practitioner-role-id"
* id = "example-practitioner-role-id"
* practitioner = Reference(PractitionerExample)
* organization = Reference(OrganizationExample)


Instance: NCPIResearchStudyExample
InstanceOf: ResearchStudy
Description: "An example representation of an NCPIResearchStudy"
* meta.profile = "http://hl7.org/fhir/StructureDefinition/ResearchStudy"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-research-study-id"
* id = "example-research-study-id"
* title = "my NCPI research study example"
* status = #active
* principalInvestigator = Reference(PractitionerRoleExample)
* condition = #DOID:0060224

Instance: PatientExample
InstanceOf: Patient
Description: "An example representation of a FHIR Patient"
* meta.profile = "http://hl7.org/fhir/StructureDefinition/Patient"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "pt-001"
* id = "pt-001"

Instance: NCPIResearchSubjectExample
InstanceOf: ResearchSubject
Description: "An example representation of an NCPIResearchSubject"
* meta.profile = "http://hl7.org/fhir/StructureDefinition/ResearchSubject"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-research-subject-id"
* id = "example-research-subject-id"
* status = #on-study
* study = Reference(NCPIResearchStudyExample)
* individual = Reference(PatientExample)


Instance: NCPISpecimenExample
InstanceOf: Specimen
Description: "An example representation of an NCPISpecimen with reference."
* meta.profile = "http://hl7.org/fhir/StructureDefinition/Specimen"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-specimen-id"
* id = "example-specimen-id"
* status = #available
* subject = Reference(PatientExample)
* type = http://terminology.hl7.org/CodeSystem/v2-0487#TISS "Tissue"
* collection.collectedDateTime = "2019-06-15"
* collection.quantity.value = 50
* collection.quantity.unit = "uL"
* collection.bodySite = http://snomed.info/sct#21483005 "Structure of central nervous system"
* collection.bodySite.text = "Central Nervous System"
* collection.method = http://snomed.info/sct#129314006 "Biopsy - action"
* collection.bodySite.text = "Biopsy"



Instance: NCPIDocumentReferenceExample
InstanceOf: DRSDocumentReference
Description: "An example representation of a NCPIDocumentReference"
* meta.profile = "http://fhir.ncpi-project-forge.io/StructureDefinition/ncpi-drs-document-reference"
* id = "example-document-reference-id"
* status = #current
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-document-reference-id"
* content.attachment = DRSAttachmentExample
// * content.attachment.contentType = #application/json
// * content.attachment.url = "drs://example.com/ga4gh/drs/v1/objects/0f8c27b9-3300-4249-bb28-f49ffb80e277"




Instance: NCPIPatientTaskExample
InstanceOf: Task
Description: "An example representation of a Task with Specimen as an input"
* meta.profile = "http://hl7.org/fhir/StructureDefinition/Task"
* id = "example-task-id"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-task-id"
* status = #completed
* intent = #order
* input[0].type = #Specimen
* input[0].valueReference = Reference(NCPISpecimenExample)
* output[0].type = #DocumentReference
* output[0].valueReference = Reference(NCPIDocumentReferenceExample)
