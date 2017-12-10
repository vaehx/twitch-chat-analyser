#!/bin/bash

#
# Usage: start-hdfs.sh [--slave]
#	--slave		does not start a datanode on that node yet. Instead, it will wait for an
#			 	external namenode to connect via ssh and start the slave daemon.
#

source /usr/bin/xmltools.sh

xmlproperty $HADOOP_HOME/etc/hadoop/core-site.xml "fs.defaultFS" "hdfs://$HADOOP_ADVERTISED_HOSTNAME/"


if ! grep -q $HADOOP_ADVERTISED_HOSTNAME /etc/hosts; then
    # Prepend /etc/hosts to be able to reverse-lookup the advertised hostname on the machine
    # This workaround is necessary, because hadoop does not provide a way to specify an advertised hostname,
    # but relies on a properly set up DNS. However, in our development environment we don't have a proper DNS setup.
    ip_addr="$(hostname --ip-address)"
    sleep 0.2
    echo -e "$ip_addr $HADOOP_ADVERTISED_HOSTNAME\n$(cat /etc/hosts)" > /etc/hosts
fi


echo "Starting SSH server..."
service ssh start

# Wait some time for ssh server to be ready
sleep 2

# Make sure we accept localhost keys
if [ ! -f /home/$HADOOP_USERNAME/.ssh/known_hosts ]; then
    su $HADOOP_USERNAME -c 'ssh-keyscan 0.0.0.0 localhost $HADOOP_ADVERTISED_HOSTNAME >> ~/.ssh/known_hosts'
fi


if ! [ "$1" == "--slave" ]; then
	# Make sure the namenode is formatted at initial start
    if [ ! -f $HADOOP_HOME/dfs/name/current/VERSION ]; then
        echo "Formatting namenode for the first time..."
        su $HADOOP_USERNAME -m -c '$HADOOP_HOME/bin/hadoop namenode -format'
    fi

	# Start as streambench user
    echo "Starting DFS namenode and datanodes..."
	su $HADOOP_USERNAME -m -c $HADOOP_HOME/sbin/start-dfs.sh

    # Add hadoop bin directory to path
    PATH=$PATH:$HADOOP_HOME/bin

    # Make sure spark checkpoint directory exists on hdfs
    if ! hdfs dfs -test -d /spark-checkpoint; then
        # We need to execute this as hadoop user, because root does not have
        # permissions in root directory
        su $HADOOP_USERNAME -m -c '$HADOOP_HOME/bin/hdfs dfs -mkdir /spark-checkpoint'
    fi
fi

# Do not exit yet
while true; do sleep 1000; done
