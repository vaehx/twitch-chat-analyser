#!/bin/bash

# Description:
#	Docker helper script for waiting for specific ports to open on specific hosts
# 	and additionally adding a delay until executing the command.
# Requirements:
#	bash
# Author:
#	prosenkranz
# Usage:
#	Example: Wait for zookeeper to be available before starting Kafka broker
#		wait-for.sh --wait-for zookeeper:2181 --delay 10 -- start-kafka.sh

while [[ $1 == --* ]]; do
	case $1 in
		--wait-for)
			if [ $# -lt 3 ]; then
				echo "Missing argument for --wait-for option"
				exit 1
			fi

			host=$(echo "$2" | cut -d ':' -f 1)
			port=$(echo "$2" | cut -d ':' -f 2)
			until nc -z $host $port; do
				echo "Waiting for $host:$port to be available..."
				sleep 3
			done
			shift 2
			;;
		--delay)
			if [ $# -lt 3 ]; then
				echo "Missing argument for --delay option (the delay in seconds)"
				exit 1
			fi

			echo "Delaying for $2 seconds..."
			sleep $2
			shift 2
			;;
		--)
			shift
			;;
	esac
done

$@
