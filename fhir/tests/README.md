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
