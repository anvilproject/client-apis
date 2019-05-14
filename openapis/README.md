
## Overview

### Downloads

* [gen3.indexd (files)](https://raw.githubusercontent.com/uc-cdis/indexd/master/openapis/swagger.yaml)

* [gen3.peregrine (graphql)](https://raw.githubusercontent.com/uc-cdis/peregrine/master/openapis/swagger.yaml)

* [gen3.sheepdog (submissions)](https://raw.githubusercontent.com/uc-cdis/sheepdog/master/openapi/swagger.yml)

* [terra.leonardo (cluster mgt)](https://notebooks.firecloud.org/api-docs.yaml)

* [dockstore (workflow)](https://raw.githubusercontent.com/dockstore/dockstore/master/dockstore-webservice/src/main/resources/swagger.yaml)

### Generates

* Single, combined swagger doc `output/combined-apis.yml`
* Provenance for each source (git hash or md5sum) `provenance/*.commit`
* Aggregation is controlled by rules: see `config/combined_apis.ymls` for more see [swagger-combine](https://www.npmjs.com/package/swagger-combine#configuration)


### Tests

* It should generate a combined file
* Each of the expected sources should be present in combined file
* A provenance record for each should be present
* Each source should have an expected number of endpoints


## Build

```
$ cd openapis
$ docker build -t swagger-combine .
```

## Run

````
# get all input
docker  run --rm  -it -v $(pwd):/app  swagger-combine  bash bin/get_all

# apply rules, combine into combined-apis.yml
docker  run --rm -it -v $(pwd):/app  swagger-combine \
  bin/combine config/swagger-combine.yml  --includeDefinitions -o output/combined-apis.yml

# test
docker  run --rm -it -v $(pwd):/app  swagger-combine   \
  python -m pytest tests

# run all at once
docker  run  --rm -it -v $(pwd):/app  swagger-combine  bash -c \
    "bin/get_all &&  \
    bin/combine config/swagger-combine.yml  --includeDefinitions -o output/combined-apis.yml &&  \
    python -m pytest tests"
```    
