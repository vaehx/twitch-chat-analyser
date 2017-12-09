#!/bin/bash

source /usr/bin/xmltools.sh

xmlproperty /hbase/conf/hbase-site.xml "hbase.zookeeper.quorum" "$HADOOP_ADVERTISED_HOSTNAME"


# Modify /etc/hosts, so we can use the docker-host hostname internally
if ! grep -q $HADOOP_ADVERTISED_HOSTNAME /etc/hosts; then
    ip_addr="$(hostname --ip-address)"
    sleep 0.1
    echo -e "$ip_addr $HADOOP_ADVERTISED_HOSTNAME\n$(cat /etc/hosts)" > /etc/hosts
fi

# Start SSH Server
echo "Starting SSH server..."
service ssh start
sleep 2

# Make sure we accept localhost keys
if [ ! -f /home/$HADOOP_USERNAME/.ssh/known_hosts ]; then
    su $HADOOP_USERNAME -c 'ssh-keyscan 0.0.0.0 localhost $HADOOP_ADVERTISED_HOSTNAME >> ~/.ssh/known_hosts'
fi

# Wait until HDFS is live
until nc -z localhost 8020; do
    echo "Waiting for HDFS to start up..."
    sleep 2.5
done

# Start hbase
su $HADOOP_USERNAME -m -c /hbase/bin/start-hbase.sh

# Don't exit yet
while true; do sleep 1000; done
