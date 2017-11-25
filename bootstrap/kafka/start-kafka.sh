#!/bin/bash

# This script overrides kafka server properties, which can optionally be set via environment vars

KAFKA_PROPERTIES_FILE="$KAFKA_HOME/config/server.properties"


# property <key> <env-value> <default>
function property {
    if [[ -n $2 ]]; then val="$2"; else val="$3"; fi
    if grep -q "^$1" $KAFKA_PROPERTIES_FILE; then
        sed -i "/^$1/c\\$1=$val" $KAFKA_PROPERTIES_FILE
    else
        echo "$1=$val" >> $KAFKA_PROPERTIES_FILE
    fi
}


property "broker.id" "$KAFKA_BROKER_ID" "0"
property "listeners" "$KAFKA_LISTENERS" "PLAINTEXT://:9092"
property "advertised.listeners" "$KAFKA_ADVERTISED_LISTENERS" "null"

# Only one partition
property "num.partitions" "$KAFKA_NUM_PARTITIONS" 1

# 500MiB
property "log.retention.bytes" "$KAFKA_LOG_RETENTION_BYTES" 524288000

# 1GiB
property "log.segment.bytes" "$KAFKA_LOG_SEGMENT_BYTES" 1073741824

# 30s
property "log.retention.check.interval.ms" "$KAFKA_LOG_RETENTION_CHECK_MS" 30000


# Wait for zookeeper server to start up
until nc -z localhost 2181; do
    echo "Waiting for zookeeper to start up..."
    sleep 0.2
done

# Start kafka server
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_PROPERTIES_FILE
