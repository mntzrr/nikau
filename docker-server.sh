#!/bin/sh

if [ -z "$1" ]; then
    echo "Syntax: $0 <docker-tag>"
    exit 1
fi

LOG_LEVEL=${LOG_LEVEL:=info}

docker run -it \
       --privileged \
       --network host \
       -v /dev/input:/dev/input \
       -v /root/.config/monux:$HOME/.config/monux \
       "ghcr.io/nickbp/monux:$1" \
       /bin/sh -c "LOG_LEVEL=$LOG_LEVEL /monux server"
