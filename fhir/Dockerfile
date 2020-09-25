# use jekyll as a base
FROM jekyll/builder

# add node
RUN curl -sL https://deb.nodesource.com/setup_13.x | bash -
RUN apk add  nodejs 
# add the sushi tool
RUN  npm install -g fsh-sushi@0.16.0


# add java
RUN apk add --no-cache openjdk8
ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk
ENV PATH $PATH:/usr/lib/jvm/java-1.8-openjdk/jre/bin:/usr/lib/jvm/java-1.8-openjdk/bin

# https://github.com/docker-library/openjdk/issues/73#issuecomment-451102068
RUN apk add --no-cache ttf-dejavu


# add misc
RUN apk add curl
RUN apk add jq


# anticipate source mounted to /src
VOLUME [ "/src" ]
WORKDIR /src


