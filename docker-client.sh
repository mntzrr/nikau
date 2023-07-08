#!/bin/sh

if [ -z "$1" -o -z "$2" ]; then
    echo "Syntax: $0 <docker-tag> <server-host-or-ip>"
    exit 1
fi

LOG_LEVEL=${LOG_LEVEL:=info}

docker run -it \
       --privileged \
       --network host \
       -v /dev/input:/dev/input \
       -v /root/.config/nikau:$HOME/.config/nikau \
       "ghcr.io/nickbp/nikau:$1" \
       /bin/sh -c "LOG_LEVEL=$LOG_LEVEL /nikau client $2"
