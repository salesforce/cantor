#!/bin/sh

install() {
    build_jar
}

build_jar() {
    echo "*** running maven..."
    mvn clean install -DskipTests;
}

run_jar() {
    echo "*** running cantor..."
    java -jar cantor-server/target/cantor-server.jar cantor-server/src/main/resources/cantor-server.conf
}

prep_docker_internal() {
    echo "*** moving jar to docker folder"
    cp cantor-server/target/cantor-server.jar env/dockers/cantor/
}

build_docker() {
    prep_docker_internal

    echo "*** building cantor docker"
    docker build --tag=cantor env/dockers/cantor
}

run_docker() {
    echo "*** running cantor in docker container"
    docker run -d --publish=7443:7443 --user 7447:7447 --name=cantor cantor
}

kill_docker() {
    echo "*** killing cantor docker container"
    docker kill cantor ; docker rm -v cantor
}

set -e

if [ "$#" = 0 ]
then
    install
    run_jar
    exit
fi

for todo in "$@"
do
    $todo
done
