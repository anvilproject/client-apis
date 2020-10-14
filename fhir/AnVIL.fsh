
Instance: PrincipalInvestigatorExample
InstanceOf: Practitioner
Description: "An example representation of a PrincipalInvestigator"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-principle-investigator"
* id = "example-principle-investigator"


Instance: NCPIResearchStudyExample
InstanceOf: ResearchStudy
Description: "An example representation of an NCPIResearchStudy"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-project-id"
* id = "example-project-id"
* title = "my NCPI research study example"
* status = #active
* principalInvestigator = Reference(PrincipalInvestigatorExample)

Instance: PatientExample
InstanceOf: Patient
Description: "An example representation of a FHIR Patient"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-patient-id"
* id = "example-patient-id"

Instance: NCPIResearchSubjectExample
InstanceOf: ResearchSubject
Description: "An example representation of an NCPIResearchSubject"
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-subject-id"
* id = "example-subject-id"
* status = #on-study
* study = Reference(NCPIResearchStudyExample)
* individual = Reference(PatientExample)


Instance: NCPISpecimenExample
InstanceOf: Specimen
Description: "An example representation of an NCPISpecimen with reference."
* identifier.system = "urn:ncpi:unique-string"
* identifier.value = "example-sample-id"
* id = "example-sample-id"
* status = #available
* subject = Reference(PatientExample)


Instance: NCPIDocumentReferenceResearchStudyExample
InstanceOf: DocumentReference
Description: "An example representation of a NCPIDocumentReference"
* id = "example-research-study-document-reference-id"
* content[0].attachment = DRSAttachmentExample
* status = #current
* identifier.system = "urn:ncpi:example-research-study-document-reference-id"
* identifier.value = "example-research-study-document-reference-id"

Instance: NCPIPatientTaskExample
InstanceOf: Task
Description: "An example representation of a Task with ResearchStudy as an inpuit"
* id = "example-research-study-task-id"
* status = #accepted
* intent = #unknown
* input[0].type = #ResearchStudy
* input[0].valueReference = Reference(NCPIResearchStudyExample)
* output[0].type = #DocumentReference
* output[0].valueReference = Reference(NCPIDocumentReferenceResearchStudyExample)
