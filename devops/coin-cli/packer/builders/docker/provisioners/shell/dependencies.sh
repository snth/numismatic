#!/bin/ash -ex
apk update; apk upgrade; apk add \
    bash \
    bash-completion \
    git \
    python3-dev \
    sudo
addgroup user
adduser -S user
