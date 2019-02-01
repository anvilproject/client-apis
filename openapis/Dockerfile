# creates a minimal node env to run swagger-combine

FROM node:alpine

RUN apk add --update \
    python \
    python-dev \
    py-pip \
    build-base \
    jpeg-dev zlib-dev \
    bash \
    git \
  && rm -rf /var/cache/apk/*

# copy current directory
WORKDIR /app
COPY . /app

# setup python env

RUN pip install -r requirements.txt

# setup npm
ENV NODE_PATH=/usr/local/lib/node_modules:/usr/local/lib/node
RUN npm install -g swagger-combine minimist
