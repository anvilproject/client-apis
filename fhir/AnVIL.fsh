
Extension:      AnvilDocumentReference
Id:             anvil-document-reference
Title:          "AnVIL Document Reference"
Description:    "An association to a document."
* value[x] only Reference(DRSAttachment)

Profile:        AnvilResearchStudy
Parent:         ResearchStudy
Id:             anvil-research-study
Title:          "AnVIL ResearchStudy"
Description:    "An AnVIL ResearchStudy."
* extension contains 
    AnvilDocumentReference named attachments 0..*


Profile:        AnvilResearchSubject
Parent:         ResearchSubject
Id:             anvil-research-subject
Title:          "AnVIL ResearchSubject"
Description:    "An AnVIL ResearchSubject."
* extension contains 
    AnvilDocumentReference named attachments 0..*


Profile:        AnvilSpecimen
Parent:         Specimen
Id:             anvil-specimen
Title:          "AnVIL Specimen"
Description:    "An AnVIL Specimen."
* extension contains 
    AnvilDocumentReference named attachments 0..*


Instance: AnvilResearchStudyExample
InstanceOf: AnvilResearchStudy
Description: "An example representation of an AnvilResearchStudy"
* id = "any-project-id"
* title = "my-project-title"
* status = #active
* extension[attachments][0].valueReference = Reference(DRSAttachmentExample)


Instance: PatientExample
InstanceOf: Patient
Description: "An example representation of a FHIR Patient"
* id = "any-patient-id"


Instance: AnvilResearchSubjectExample
InstanceOf: AnvilResearchSubject
Description: "An example representation of an AnvilResearchSubject"
* id = "any-subject-id"
* status = #on-study
* study = Reference(AnvilResearchStudyExample)
* individual = Reference(PatientExample)
* extension[attachments][0].valueReference = Reference(DRSAttachmentExample)


Instance: AnvilSpecimenExample
InstanceOf: AnvilSpecimen
Description: "An example representation of an AnvilSpecimen with reference."
* id = "any-sample-id"
* status = #available
* extension[attachments][0].valueReference = Reference(DRSAttachmentExample)