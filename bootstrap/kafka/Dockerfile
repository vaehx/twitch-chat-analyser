# Kafka and Zookeeper

FROM debian:stable-slim

ENV KAFKA_HOME /opt/kafka

RUN apt-get update

# Install some helpful tools during development
RUN apt-get install -y procps lsof iproute2 iputils-ping nano tcpdump

# Install Java
RUN mkdir /usr/share/man/man1
RUN apt-get install -y default-jre-headless

# Install supervisor
RUN apt-get install -y supervisor
ADD supervisor/zookeeper.conf supervisor/kafka.conf /etc/supervisor/conf.d/

# We need netcat to test if zookeeper is already up
RUN apt-get install -y netcat

# Cleanup APT cache
RUN apt-get clean

# Download and extract kafka package
ADD http://mirrors.ae-online.de/apache/kafka/0.10.2.1/kafka_2.11-0.10.2.1.tgz /tmp/kafka.tgz
RUN mkdir -p $KAFKA_HOME
RUN tar -xzvf /tmp/kafka.tgz --strip 1 -C $KAFKA_HOME
RUN rm /tmp/kafka.tgz

# Add kafka start script
ADD start-kafka.sh /usr/bin/start-kafka.sh
RUN chmod +x /usr/bin/start-kafka.sh

CMD ["supervisord", "-n"]
