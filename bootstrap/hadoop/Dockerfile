FROM debian:stable-slim

ENV HADOOP_USERNAME hadoop
ENV HADOOP_HOME /opt/hadoop
ENV HBASE_HOME /opt/hbase

RUN apt-get update
RUN apt-get -y install ssh procps whois lsof iproute2 iputils-ping tcpdump nano

# Setup supervisor
RUN apt-get install -y supervisor

# Install required packages in startup scripts
RUN apt-get install -y xmlstarlet netcat
ADD xmltools.sh /usr/bin/
RUN chmod +x /usr/bin/xmltools.sh

# Setup SSH access
RUN /etc/init.d/ssh start
RUN ssh-keygen -f ~/.ssh/id_rsa -t rsa -N '' && cp ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys
RUN adduser --gecos "" --disabled-password $HADOOP_USERNAME
RUN mkdir /home/$HADOOP_USERNAME/.ssh
RUN ssh-keygen -f /home/$HADOOP_USERNAME/.ssh/id_rsa -t rsa -N '' && cp /home/$HADOOP_USERNAME/.ssh/id_rsa.pub /home/$HADOOP_USERNAME/.ssh/authorized_keys
RUN chown -R $HADOOP_USERNAME:$HADOOP_USERNAME /home/$HADOOP_USERNAME/.ssh

# Java JRE 1.8
RUN mkdir /usr/share/man/man1
RUN apt-get -y install default-jre-headless
ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/jre/"

RUN apt-get clean

# Hadoop (HDFS, YARN, ...)
ADD http://www.gutscheine.org/mirror/apache/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz /tmp/hadoop.tar.gz
RUN mkdir $HADOOP_HOME \
	&& tar -xzf /tmp/hadoop.tar.gz --strip 1 -C $HADOOP_HOME \
	&& ln -s $HADOOP_HOME /hadoop \
	&& rm -rf /hadoop/src \
	&& rm -rf /hadoop/share/doc

ADD hadoop/hdfs-site.xml hadoop/core-site.xml hadoop/hadoop-env.sh /hadoop/etc/hadoop/

RUN mkdir -p /hadoop/dfs /hadoop/dfs/name /hadoop/dfs/data

RUN chown $HADOOP_USERNAME:$HADOOP_USERNAME $HADOOP_HOME
RUN chown -R $HADOOP_USERNAME:$HADOOP_USERNAME $HADOOP_HOME/dfs

ADD hadoop/start-hdfs.sh /usr/bin/
RUN chmod +x /usr/bin/start-hdfs.sh

# HBase
ADD http://mirrors.ae-online.de/apache/hbase/1.3.1/hbase-1.3.1-bin.tar.gz /tmp/hbase.tar.gz
RUN mkdir $HBASE_HOME \
    && tar -xzf /tmp/hbase.tar.gz --strip 1 -C $HBASE_HOME \
    && ln -s $HBASE_HOME /hbase \
    && rm -rf /hbase/docs

ADD hbase/hbase-site.xml hbase/hbase-env.sh /hbase/conf/

ADD hbase/start-hbase.sh /usr/bin/
RUN chmod +x /usr/bin/start-hbase.sh

RUN chown $HADOOP_USERNAME:$HADOOP_USERNAME $HBASE_HOME

ADD supervisor/hadoop.conf supervisor/hbase.conf /etc/supervisor/conf.d/

CMD ["supervisord", "-n"]
