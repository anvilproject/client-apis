
# Google FHIR service compliance


## Validation

```
The Cloud Healthcare API does not currently enforce all of the rules in a StructureDefinition. The following rules are supported:


- min/max
- minValue/maxValue
- maxLength
- type
- fixed[x]
- pattern[x] on simple types
- slicing, when using "value" as the discriminator type
When a URL cannot be resolved (for example, in a type assertion), the server does not return an error.
```

(see more)[https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores?hl=en#validationconfig]



https://cloud.google.com/healthcare/docs/fhir#details_of_supported_functionality_in_the_v1_api_by_fhir_version


Limited terminology lookup support

https://groups.google.com/g/gcp-healthcare-discuss/c/zZIqfplW36c/m/NFa_bkyKAAAJ



I was trying it on my instance of the Google Healthcare API service.

At a high level, trying to determine if it supports  `Remote Terminology Services`

ie. Fhir service proxies requests to remote terminology services

eg. 

    * fhir spec http://hl7.org/fhir/terminology-service.html

    * smilecdr see `diagram`  https://smilecdr.com/docs/validation_and_conformance/remote_terminology_services.html#remote-terminology-service

    * firely see `RemoteTerminologyServices` https://docs.fire.ly/projects/Firely-Server/en/latest/features/terminology.html#configuration


The Google documentation is largely silent about this topic.  There is a reference here:

    * https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores.fhir/ConceptMap-translate

    * With a single sentence "You can provide your own concept maps to translate any code system to another code system."

I've posted a message to the GCP Healthcare Discuss group, I'll let you know what I find out. 
