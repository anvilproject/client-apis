# client-APIs

## name

theanvil/bioxyz

## definition

Define clients for Python, R, javascript that interact with [terra, gen3, galaxy, others]

## goals

### short term

* Integrate swagger/openapi documents and individual client libraries to access [terra, gen3, others].
* Maintain independence of micros ervices and their respective swagger/openapi documents and individual client libraries
* Create an server side endpoint that will enable service discovery
* Create an AnVIL specific, versioned, testable client for each of the client targets
* Aggregate documentation, examples and cookbooks to inform developers
* Opinionated, given multiple ways to accomplish a function, the client will support and promote one. ex: [upload/download, get data model]

### long term

* Enable analysis on cross project cohorts.  Cross project in that data originates from multiple non-AnVIL projects.
* Add value to vendor libraries [google, aws]  

### first iteration

* auth
* minimum set of methods ( print version )
* consider swagger vs. openapi (code gen capability drift ?)
* integration tests
* revisit package name
* create CI against versioned data (assume we will have synthetic data)

### context

endpoint urls:
  * AnVIL will have a single domain hosting gen3 and terra
  * firecloud maintains different urls per micro service.  There is one instance of each micro service (multi tenant)
  * gen3 maintains different urls per micro service.  There is one instance of each micro service per project
  * galaxy maintains different urls per user in AnVIL.
