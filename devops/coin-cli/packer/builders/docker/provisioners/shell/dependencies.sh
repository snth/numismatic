#!/bin/ash -ex
apk update; apk upgrade; apk add \
    bash \
    bash-completion \
    git \
    python3-dev
addgroup user
adduser -S user
