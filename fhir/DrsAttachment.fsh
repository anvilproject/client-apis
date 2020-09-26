Profile:        DRSAttachment
Parent:         Attachment
Id:             drs-attachment
Title:          "DRS Attachment"
Description:    "A FHIR Attachment extended with DRS Object attributes."
// https://github.com/ga4gh/data-repository-service-schemas/blob/master/openapi/data_repository_service.swagger.yaml#L190-L304

// adds DRSObject to Attachment
* extension contains DRSObject named drs 0..1

// remove hash from Attachment definition
* hash 0..0



// inline definition of sub-extensions
Extension:  DRSObject
Id: drs-object
Title: "DRS Object"
Description: "The drs object"
* extension contains
    id 1..1 MS and
    name 0..1 and
    self_uri 1..1 MS and
    size 1..1 MS and
    created_time 1..1 MS and
    updated_time 0..1 and
    version 0..1 and
    mime_type 0..1 
    and DRSChecksum named checksums 1..* MS
    and DRSAccessMethod named access_methods 1..* MS

* extension[id] ^short = "An identifier unique to this `DrsObject`."
* extension[id].value[x] only string
* extension[name] ^short = "A string that can be used to name a `DrsObject`."
* extension[name].value[x] only string
* extension[self_uri] ^short = "A drs:// URI, as defined in the DRS documentation, that tells clients how to access this object."
* extension[self_uri].value[x] only string
* extension[size] ^short = "For blobs, the blob size in bytes.  For bundles, the cumulative size, in bytes, of items in the `contents` field."
* extension[size].value[x] only integer
* extension[created_time] ^short = "Timestamp of content creation in RFC3339."
* extension[created_time].value[x] only dateTime
* extension[updated_time] ^short = "Timestamp of content update in RFC3339, identical to `created_time` in systems that do not support updates."
* extension[updated_time].value[x] only dateTime
* extension[version] ^short = "A string representing a version. (Some systems may use checksum, a RFC3339 timestamp, or an incrementing version number.)"
* extension[version].value[x] only string
* extension[mime_type] ^short = "A string providing the mime-type of the `DrsObject`."
* extension[mime_type].value[x] only string


Extension:  DRSChecksum
Id: drs-checksum
Title: "DRS Checksum"
Description: "The checksum of the `DrsObject`. At least one checksum must be provided."    
* extension contains
    checksum 1..1 MS and
    type 1..1 MS
* extension[checksum] ^short = "The hex-string encoded checksum for the data."
* extension[checksum].value[x] only string
* extension[type] ^short = "The digest method used to create the checksum."
* extension[type].value[x] only string


Extension:  DRSAccessMethod
Id: drs-access-method
Title: "DRS AccessMethod"
Description: "The list of access methods that can be used to fetch the `DrsObject`."    
* extension contains
    type 1..1 MS and
    access_url 0..1 and
    access_id 0..1 and
    region 0..1
* extension[type] ^short = "Type of the access method."
* extension[type].value[x] only string
* extension[access_url] ^short = "An `AccessURL` that can be used to fetch the actual object bytes."
* extension[access_url].value[x] only string
* extension[access_id] ^short = "An arbitrary string to be passed to the `/access` method to get an `AccessURL`."
* extension[access_id].value[x] only string
* extension[region] ^short = "An arbitrary string to be passed to the `/access` method to get an `AccessURL`."
* extension[region].value[x] only string


Instance: DRSAttachmentExample
InstanceOf: DRSAttachment
Description: "An example representation of a DRSAttachment"
Usage: #inline
* id = "any-attachment-id"
* contentType = #application/json
* extension[drs].extension[id].valueString = "any-id"
* extension[drs].extension[name].valueString = "any-file-name"
* extension[drs].extension[self_uri].valueString = "drs://url-here"
* extension[drs].extension[created_time].valueDateTime = "1985-04-12T23:20:50.52Z"
* extension[drs].extension[updated_time].valueDateTime = "1985-04-12T23:20:50.52Z"
* extension[drs].extension[size].valueInteger = 12345
* extension[drs].extension[version].valueString = "0.0.0"
* extension[drs].extension[mime_type].valueString = "application/json"
* extension[drs].extension[checksums].extension[checksum].valueString = "abcdef0123456789"
* extension[drs].extension[checksums].extension[type].valueString = "etag"
* extension[drs].extension[access_methods].extension[type].valueString = "s3"
* extension[drs].extension[access_methods].extension[access_url].valueString = "s3://some-url-here"
* extension[drs].extension[access_methods].extension[region].valueString = "us-west"



