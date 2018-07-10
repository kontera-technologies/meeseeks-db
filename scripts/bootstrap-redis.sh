#!/usr/bin/env bash

function start_redis {
    local name=$1
    local port=$2

    # remove old container if it exists
    docker rm -f -v $name &> /dev/null

    # start container
    docker run -v ${PWD}/etc/dev-test/redis.conf:/usr/local/etc/redis.conf --name $name -p $port:6379 -d redis redis-server /usr/local/etc/redis.conf
}

start_redis meeseeks-db-redis0 26269
start_redis meeseeks-db-redis1 26279
start_redis meeseeks-db-redis2 26289
