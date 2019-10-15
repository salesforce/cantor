#!/bin/bash

# set x so we can see the actual java command run
set -x

# first start maiev
/bin/sh start_maiev.sh &>/dev/null &

if [[ -n ${MYSQL_SHARDS} ]]; then
    # evaluate referenced environment variables
    mysql_shards=$(eval echo ${MYSQL_SHARDS})
    echo "running cantor with mysql shards: $mysql_shards"
    export MYSQL_SHARDS="$mysql_shards"
fi

# start the cantor server with all parameters passed to command line
java -jar -Dlogback.configurationFile=./cantor-logback.xml cantor-server.jar $@
