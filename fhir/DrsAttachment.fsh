// Invariant:  must-be-ncpi-drs-attachment
// Description: "The attachment must be a ncpi-drs-attachment"
// Expression: "$this.content.attachment.extension('http://fhir.ncpi-project-forge.io/StructureDefinition/ncpi-drs-attachment')"
// Severity:   #error

Profile:        DRSDocumentReference
Parent:         DocumentReference
Id:             ncpi-drs-document-reference
Title:          "DRS Document Reference"
Description:    "A FHIR Document Reference with an embedded DRS URI. See https://github.com/ga4gh/data-repository-service-schemas"
* content.attachment only DRSAttachment

Invariant:  must-be-drs-uri
Description: "attachment.url must start with ^drs://. A drs:// hostname-based URI, as defined in the DRS documentation, that tells clients how to access this object. The intent of this field is to make DRS objects self-contained, and therefore easier for clients to store and pass around.  For example, if you arrive at this DRS JSON by resolving a compact identifier-based DRS URI, the `self_uri` presents you with a hostname and properly encoded DRS ID for use in subsequent `access` endpoint calls."
Expression: "$this.url.matches('^drs://.*')"
Severity:   #error

Profile:        DRSAttachment
Parent:         Attachment
Id:             ncpi-drs-attachment
Title:          "DRS Attachment"
Description:    "A FHIR Attachment a DRS url."
// https://github.com/ga4gh/data-repository-service-schemas/blob/master/openapi/data_repository_service.swagger.yaml#L190-L304
* obeys must-be-drs-uri

Instance: DRSAttachmentExample
InstanceOf: DRSAttachment
Description: "An example representation of a DRSAttachment"
Usage: #inline
// * meta.profile = "http://fhir.ncpi-project-forge.io/StructureDefinition/ncpi-drs-attachment"
// The element or path you referenced does not exist: meta.profile

// * extension = ["http://fhir.ncpi-project-forge.io/StructureDefinition/drs-uri"]
// Cannot find definition for Instance: ["http://fhir.ncpi-project-forge.io/StructureDefinition/drs-uri"].

* url = "drs://example.com/ga4gh/drs/v1/objects/0f8c27b9-3300-4249-bb28-f49ffb80e277"
