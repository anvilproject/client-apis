Tests in this directory:
* deploy generated FHIR artifacts to test smilecdr server
* test validity of those artifacts
* extract an AnVIL Terra workspace,
* transform to FHIR resources
* deploy the entities to the FHIR server


Starting the test server:

```
# retrieve image from KF docker hub
  export DOCKER_HUB_USERNAME=...
  export DOCKER_HUB_PW=..
  export DOCKER_CONTAINER='fhir-test-server'
  export DOCKER_IMAGE='kidsfirstdrc/smilecdr:2020.05.PRE-14-test'  

# run the server
# see scripts/integration_test.sh, but the key command is:

docker run -d --rm --name "$DOCKER_CONTAINER" -p 8001:8001  -p 8000:8000 -p 9000:9000 -p 9100:9100 "$DOCKER_IMAGE"

# build our fish, integration guide generator
docker build -t fish .
alias fish='docker run -v $(pwd)/:/src --rm -it fish'
# generate the FHIR profiles, examples etc.
fish sushi .

# run the tests, which will deploy generated config to FHIR server & test workspace transform
pytest --log-cli-level DEBUG tests/test_fhir.py  --user_project <your terra google billing project>

# for collaboration store the generated items

cp -r build/input/examples/ fhir-artifacts/examples/
cp -r build/input/extensions/ fhir-artifacts/extensions/
cp -r build/input/profiles/ fhir-artifacts/profiles/

```

Issues

----

# Summary
Smile CDR throws stack trace on DocumentReference Resource Create.

# Use Case

Include object store (google, s3, azure, ... ) bucket information with FHIR DocumentReferences.

Mapping: A  FHIR [Attachment](https://www.hl7.org/fhir/attachment.profile.json.html) is extended to include remote repository information corresponding to the [GA4GH DRS format](https://github.com/ga4gh/data-repository-service-schemas/blob/master/openapi/data_repository_service.swagger.yaml#L190-L304).

 
## Artifacts

### FSH source

The profile was modeled using [Sushi](https://github.com/FHIR/sushi) using the  [FSH shorthand](https://github.com/anvilproject/client-apis/blob/fhir/fhir/DrsAttachment.fsh)


### Generated FHIR Structure definitions

* [extensions](https://github.com/anvilproject/client-apis/tree/fhir/fhir/fhir-artifacts/extensions)
* [profiles](https://github.com/anvilproject/client-apis/tree/fhir/fhir/fhir-artifacts/profiles)
* [examples](https://github.com/anvilproject/client-apis/tree/fhir/fhir/fhir-artifacts/examples)


# Issue 

## error

`smilecdr` errors when DocumentReference includes a DRSAttachement

![image](https://user-images.githubusercontent.com/47808/94343690-b0a26500-ffce-11ea-9966-e26df8cbc9cb.png)

## however

If `extension in an extension` is removed from profile the update works

```
http://localhost:8000/DocumentReference/example-document-reference-id 200 {
  "resourceType": "DocumentReference",
  "id": "example-document-reference-id",
  "meta": {
    "versionId": "1",
    "lastUpdated": "2020-09-26T00:30:47.518+00:00",
    "source": "#OqQrYiIyBmdnHlEi"
  },
  "status": "current",
  "content": [ {
    "attachment": {
      "id": "any-attachment-id",
      "extension": [ {
        "url": "http://anvilproject.org/StructureDefinition/drs-object",
        "extension": [ {
          "url": "id",
          "valueString": "any-id"
        }, {
          "url": "self_uri",
          "valueString": "drs://url-here"
        }, {
          "url": "size",
          "valueInteger": 12345
        }, {
          "url": "created_time",
          "valueDateTime": "1985-04-12T23:20:50.52Z"
        }, {
          "url": "name",
          "valueString": "any-file-name"
        }, {
          "url": "updated_time",
          "valueDateTime": "1985-04-12T23:20:50.52Z"
        }, {
          "url": "version",
          "valueString": "0.0.0"
        }, {
          "url": "mime_type",
          "valueString": "application/json"
        } ]
      } ],
      "contentType": "application/json"
    }
  } ]
}
```

### `extension in an extension` 
```
diff --git a/fhir/DrsAttachment.fsh b/fhir/DrsAttachment.fsh
index b5013c9..426e5e8 100644
--- a/fhir/DrsAttachment.fsh
+++ b/fhir/DrsAttachment.fsh
@@ -27,8 +27,8 @@ Description: "The drs object"
     updated_time 0..1 and
     version 0..1 and
     mime_type 0..1 
-    and DRSChecksum named checksums 1..* MS
-    and DRSAccessMethod named access_methods 1..* MS
+    // and DRSChecksum named checksums 1..* MS
+    // and DRSAccessMethod named access_methods 1..* MS
 
 * extension[id] ^short = "An identifier unique to this `DrsObject`."
 * extension[id].value[x] only string
@@ -94,11 +94,11 @@ Usage: #inline
 * extension[drs].extension[size].valueInteger = 12345
 * extension[drs].extension[version].valueString = "0.0.0"
 * extension[drs].extension[mime_type].valueString = "application/json"
-* extension[drs].extension[checksums].extension[checksum].valueString = "abcdef0123456789"
-* extension[drs].extension[checksums].extension[type].valueString = "etag"
-* extension[drs].extension[access_methods].extension[type].valueString = "s3"
-* extension[drs].extension[access_methods].extension[access_url].valueString = "s3://some-url-here"
-* extension[drs].extension[access_methods].extension[region].valueString = "us-west"
+// * extension[drs].extension[checksums].extension[checksum].valueString = "abcdef0123456789"
+// * extension[drs].extension[checksums].extension[type].valueString = "etag"
+// * extension[drs].extension[access_methods].extension[type].valueString = "s3"
+// * extension[drs].extension[access_methods].extension[access_url].valueString = "s3://some-url-here"
+// * extension[drs].extension[access_methods].extension[region].valueString = "us-west"
 
 
 
diff --git a/fhir/fhir-artifacts/examples/DocumentReference-example-document-reference-id.json b/fhir/fhir-artifacts/examples/DocumentReference-example-document-reference-id.json
index e774db0..eed9dbc 100644
--- a/fhir/fhir-artifacts/examples/DocumentReference-example-document-reference-id.json
+++ b/fhir/fhir-artifacts/examples/DocumentReference-example-document-reference-id.json
@@ -23,36 +23,6 @@
                 "url": "created_time",
                 "valueDateTime": "1985-04-12T23:20:50.52Z"
               },
-              {
-                "url": "http://anvilproject.org/StructureDefinition/drs-checksum",
-                "extension": [
-                  {
-                    "url": "checksum",
-                    "valueString": "abcdef0123456789"
-                  },
-                  {
-                    "url": "type",
-                    "valueString": "etag"
-                  }
-                ]
-              },
-              {
-                "url": "http://anvilproject.org/StructureDefinition/drs-access-method",
-                "extension": [
-                  {
-                    "url": "type",
-                    "valueString": "s3"
-                  },
-                  {
-                    "url": "access_url",
-                    "valueString": "s3://some-url-here"
-                  },
-                  {
-                    "url": "region",
-                    "valueString": "us-west"
-                  }
-                ]
-              },
               {
                 "url": "name",
                 "valueString": "any-file-name"
diff --git a/fhir/fhir-artifacts/extensions/StructureDefinition-drs-object.json b/fhir/fhir-artifacts/extensions/StructureDefinition-drs-object.json
index c7bbff6..ebf8eb9 100644
--- a/fhir/fhir-artifacts/extensions/StructureDefinition-drs-object.json
+++ b/fhir/fhir-artifacts/extensions/StructureDefinition-drs-object.json
@@ -31,7 +31,7 @@
       {
         "id": "Extension.extension",
         "path": "Extension.extension",
-        "min": 6
+        "min": 4
       },
       {
         "id": "Extension.extension:id",
@@ -253,38 +253,6 @@
           }
         ]
       },
-      {
-        "id": "Extension.extension:checksums",
-        "path": "Extension.extension",
-        "sliceName": "checksums",
-        "min": 1,
-        "max": "*",
-        "type": [
-          {
-            "code": "Extension",
-            "profile": [
-              "http://anvilproject.org/StructureDefinition/drs-checksum"
-            ]
-          }
-        ],
-        "mustSupport": true
-      },
-      {
-        "id": "Extension.extension:access_methods",
-        "path": "Extension.extension",
-        "sliceName": "access_methods",
-        "min": 1,
-        "max": "*",
-        "type": [
-          {
-            "code": "Extension",
-            "profile": [
-              "http://anvilproject.org/StructureDefinition/drs-access-method"
-            ]
-          }
-        ],
-        "mustSupport": true
-      },
       {
         "id": "Extension.url",
         "path": "Extension.url",

```