Extension:      AnvilDocumentReference
Id:             anvil-document-reference
Title:          "AnVIL Document Reference"
Description:    "An association to a document."
* value[x]	only Reference(DocumentReference)


Profile:        AnvilResearchStudy
Parent:         ResearchStudy
Id:             anvil-research-study
Title:          "AnVIL ResearchStudy"
Description:    "An AnVIL ResearchStudy."
* extension contains 
    AnvilDocumentReference named documents 0..*

Profile:        AnvilResearchSubject
Parent:         ResearchSubject
Id:             anvil-research-subject
Title:          "AnVIL ResearchSubject"
Description:    "An AnVIL ResearchSubject."
* extension contains 
    AnvilDocumentReference named documents 0..*

Profile:        AnvilSpecimen
Parent:         Specimen
Id:             anvil-specimen
Title:          "AnVIL Specimen"
Description:    "An AnVIL Specimen."
* extension contains 
    AnvilDocumentReference named documents 0..*


Instance: AnvilDocumentReferenceExample
InstanceOf: DocumentReference
Description: "An example representation of a AnvilDocumentReference"
* id = "example-document-reference-id"
* content[0].attachment = DRSAttachmentExample
* status = #current
* identifier.system = "urn:anvil:unique-string"
* identifier.value = "example-document-reference-id"


Instance: PrincipalInvestigatorExample
InstanceOf: Practitioner
Description: "An example representation of a PrincipalInvestigator"
* identifier.system = "urn:anvil:unique-string"
* identifier.value = "example-principle-investigator"
* id = "example-principle-investigator"


Instance: AnvilResearchStudyExample
InstanceOf: AnvilResearchStudy
Description: "An example representation of an AnvilResearchStudy"
* identifier.system = "urn:anvil:unique-string"
* identifier.value = "example-project-id"
* id = "example-project-id"
* title = "my anvil research study example"
* status = #active
* principalInvestigator = Reference(PrincipalInvestigatorExample)
* extension[documents][0].valueReference = Reference(AnvilDocumentReferenceExample)

Instance: PatientExample
InstanceOf: Patient
Description: "An example representation of a FHIR Patient"
* identifier.system = "urn:anvil:unique-string"
* identifier.value = "example-patient-id"
* id = "example-patient-id"
// no extension here, created so ResearchSubject can reference it

Instance: AnvilResearchSubjectExample
InstanceOf: AnvilResearchSubject
Description: "An example representation of an AnvilResearchSubject"
* identifier.system = "urn:anvil:unique-string"
* identifier.value = "example-subject-id"
* id = "example-subject-id"
* status = #on-study
* study = Reference(AnvilResearchStudyExample)
* individual = Reference(PatientExample)
* extension[documents][0].valueReference = Reference(AnvilDocumentReferenceExample)


Instance: AnvilSpecimenExample
InstanceOf: AnvilSpecimen
Description: "An example representation of an AnvilSpecimen with reference."
* identifier.system = "urn:anvil:unique-string"
* identifier.value = "example-sample-id"
* id = "example-sample-id"
* status = #available
* extension[documents][0].valueReference = Reference(AnvilDocumentReferenceExample)
* subject = Reference(PatientExample)

