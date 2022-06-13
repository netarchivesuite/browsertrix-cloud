#!/bin/bash

mkdir -p /etc/nginx/resolvers/
echo resolver $(awk 'BEGIN{ORS=" "} $1=="nameserver" {print $2}' /etc/resolv.conf) valid=30s ";" > /etc/nginx/resolvers/resolvers.conf

cat /etc/nginx/resolvers/resolvers.conf

/docker-entrypoint.sh "$@"


