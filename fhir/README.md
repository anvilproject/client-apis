

# AnVIL FHIR

![image](https://user-images.githubusercontent.com/47808/80027524-c00a0900-8498-11ea-8a30-a5d340995a6c.png)


## use case

![image](https://user-images.githubusercontent.com/47808/80027617-e29c2200-8498-11ea-84a8-91c5b974c53a.png)

## getting started

Pre requisites
* docker

After cloning this repo

```
# build development docker image
docker build -t fish .

# test the image
docker run --rm -it fish sh -c "java -version && node  --version && sushi --version && jekyll --version"
>>> should product output similar to
openjdk version "1.8.0_242"
OpenJDK Runtime Environment (IcedTea 3.15.0) (Alpine 8.242.08-r0)
OpenJDK 64-Bit Server VM (build 25.242-b08, mixed mode)
v12.15.0
v0.12.2
ruby 2.7.1p83 (2020-03-31 revision a0c7c23c9c) [x86_64-linux-musl]
jekyll 4.0.0

# mount the current directory and verify contents
docker run -v $(pwd)/:/src --rm -it fish ls -1
>>> should product output similar to
Dockerfile
DrsAttachment.fsh
README.md
package.json
```

`Congrats!`. Now we are ready to develop models.

```
# setup an alias
alias fish='docker run -v $(pwd)/:/src --rm -it fish'

# enter shell
fish bash
#  once in shell, validate the model
sushi . 
# and optionally build the site
cd build
./_updatePublisher.sh
./_genonce.sh 
 
```

## update ncpi model

```
cp build/input/examples/*.* ncpi-model-forge/site_root/input/resources/examples
cp build/input/extensions/*.* ncpi-model-forge/site_root/input/resources/extensions
cp build/input/profiles/*.* ncpi-model-forge/site_root/input/resources/profiles/
```


## testing

see tests/README.md

TODO

See  BundleVisualizer  ... https://fhirblog.com/2020/02/10/family-fhir-with-sushi/



