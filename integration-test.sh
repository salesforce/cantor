#!/usr/bin/env bash

# Spins up a Cantor server with the requested backend via Docker, runs tests and tears down.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CONFIG_FILE=""
TYPE=""
STORAGE=""
SELECT=""
PROVIDER=""

helpMessage() {
  cat <<EOF
Usage: ./integration-test.sh --type <TYPE> [--config <FILE>]

Spins up a Cantor server with the requested backend and runs integration tests.

Options:
  -t, --type TYPE       Storage type. One of: CantorOnH2, CantorOnMySQL, CantorOnS3,
                         CantorOnMulticloudj. Defaults to CantorOnH2 if omitted.
  -s, --select SELECT   Only for CantorOnS3. Defines which Select implementation the server
                         should use: 's3' (S3 Select) or 'local'
                         (Client-side Select). Defaults to 's3'. Ignores for H2/MySQL.
  -p, --provider PROVIDER  Only for CantorOnMulticloudj. Defines which cloud provider
                         the server should use: 'aws', 'gcp', or 'ali'.
                         Defaults to 'aws'. Ignored for other storage types.
  -c, --config FILE     Path to a cantor-server.conf file.
                         Defaults to env/dockers/cantor/cantor-server.conf
  -h, --help            Show this help message and exit

Examples:
  ./integration-test.sh --type CantorOnH2
  ./integration-test.sh --type CantorOnS3 --select s3
  ./integration-test.sh --type CantorOnS3 --select local
  ./integration-test.sh --type CantorOnMulticloudj --provider aws
EOF
}

errorMessage() {
  cat <<EOF
Unknown option. See available options below.

Options:
  -t, --type TYPE       Storage type. One of: CantorOnH2, CantorOnMySQL, CantorOnS3,
                         CantorOnMulticloudj. Defaults to CantorOnH2 if omitted.
  -s, --select SELECT   Only for CantorOnS3. Defines which Select implementation the server
                         should use: 's3' (S3 Select) or 'local'
                         (Client-side Select). Defaults to 's3'. Ignores for H2/MySQL.
  -p, --provider PROVIDER  Only for CantorOnMulticloudj. Defines which cloud provider
                         the server should use: 'aws', 'gcp', or 'ali'.
                         Defaults to 'aws'. Ignored for other storage types.
  -c, --config FILE     Path to a cantor-server.conf file.
                         Defaults to env/dockers/cantor/cantor-server.conf
  -h, --help            Show the help message and exit

EOF
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -t|--type)
            TYPE="$2"
            shift 2
            ;;
        -s|--select)
            SELECT="$2"
            shift 2
            ;;
        -p|--provider)
            PROVIDER="$2"
            shift 2
            ;;
        -h|--help)
            helpMessage
            exit 0
            ;;
        *)
            errorMessage
            exit 1
            ;;
    esac
done

# Use default config if not provided
if [ -z "$CONFIG_FILE" ]; then
    CONFIG_FILE="${REPO_ROOT}/env/dockers/cantor/cantor-server.conf"
fi

if [ -z "${TYPE}" ]; then
    echo "-t/--type flag is missing. Defaulting storage type to H2."
    TYPE="CantorOnH2"
    STORAGE="h2"
elif [ "${TYPE}" == "CantorOnH2" ]; then
    STORAGE="h2"
elif [ "${TYPE}" == "CantorOnMySQL" ]; then
    STORAGE="mysql"
elif [ "${TYPE}" == "CantorOnS3" ]; then
    STORAGE="s3"
elif [ "${TYPE}" == "CantorOnMulticloudj" ]; then
    STORAGE="multicloudj"
fi

if [ "$STORAGE" == "s3" ]; then
    if [ -z "$SELECT" ]; then
        echo "-s/--select flag is not provided; defaulting to 's3'"
        SELECT="s3"
    fi
    case "$SELECT" in
        s3)   TYPE="CantorOnS3-S3Select" ;;
        local) TYPE="CantorOnS3-LocalSelect" ;;
        *)     echo "Invalid select type. Use 's3' or 'local'."; exit 1 ;;
    esac
    export CANTOR_S3_SELECT_TYPE="$SELECT"
    echo "Using $SELECT-select"
fi

if [ "$STORAGE" == "multicloudj" ]; then
    if [ -z "$PROVIDER" ]; then
        echo "-p/--provider flag is not provided; defaulting to 'aws'"
        PROVIDER="aws"
    fi
    case "$PROVIDER" in
        aws|gcp|ali) ;;
        *) echo "Invalid provider. Use 'aws', 'gcp', or 'ali'."; exit 1 ;;
    esac
    TYPE="CantorOnMulticloudj-${PROVIDER}"
    export CANTOR_MULTICLOUDJ_PROVIDER="$PROVIDER"
    echo "Using $PROVIDER provider"
fi

echo "Using config: $CONFIG_FILE"
echo "Using Storage: $STORAGE"

# Export config path and storage type
export CANTOR_CONFIG_FILE="$CONFIG_FILE"
export CANTOR_STORAGE_TYPE="$STORAGE"

# Start MySQL if needed
if [ "$STORAGE" == "mysql" ]; then
    echo "Starting MySQL container..."
    docker run -d \
        --name mysql \
        --publish 3306:3306 \
        -e MYSQL_ALLOW_EMPTY_PASSWORD=yes \
        mysql:8.4
    sleep 10
fi

export CANTOR_S3_BUCKET="${CANTOR_S3_BUCKET:-bucket-place-holder}"
export CANTOR_S3_REGION="${CANTOR_S3_REGION:-us-west-2}"

export CANTOR_MULTICLOUDJ_BUCKET="${CANTOR_MULTICLOUDJ_BUCKET:-bucket-place-holder}"
export CANTOR_MULTICLOUDJ_REGION="${CANTOR_MULTICLOUDJ_REGION:-us-west-2}"
export CANTOR_MULTICLOUDJ_SETS_TYPE="${CANTOR_MULTICLOUDJ_SETS_TYPE:-h2}"
export CANTOR_MULTICLOUDJ_ACCESS_KEY_ID="${CANTOR_MULTICLOUDJ_ACCESS_KEY_ID:-}"
export CANTOR_MULTICLOUDJ_SECRET_ACCESS_KEY="${CANTOR_MULTICLOUDJ_SECRET_ACCESS_KEY:-}"
export CANTOR_MULTICLOUDJ_SESSION_TOKEN="${CANTOR_MULTICLOUDJ_SESSION_TOKEN:-}"

# Build cantor-server jar if it doesn't exist
SERVER_JAR="${REPO_ROOT}/env/dockers/cantor/cantor-server.jar"
if [ ! -f "${SERVER_JAR}" ]; then
    echo "Building cantor-server.jar"

    cd "${REPO_ROOT}"
    mvn -q clean package -DskipTests -Dgpg.skip=true -Dmaven.javadoc.skip=true -Dsource.skip=true -pl cantor-server -am

    cp cantor-server/target/cantor-server.jar "${SERVER_JAR}"
    echo "cantor-server.jar built successfully."
fi

cd "${REPO_ROOT}/cantor-integration"

docker-compose up -d --build cantor-server
docker-compose run --build --rm cantor-client --type ${TYPE}

EXIT_CODE=$?

docker-compose down
if [ "$STORAGE" == "mysql" ]; then
    echo "Removing MySQL container..."
    docker rm -f mysql
fi

exit ${EXIT_CODE}