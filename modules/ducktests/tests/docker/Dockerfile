# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG jdk_version=openjdk:8
FROM $jdk_version

MAINTAINER Apache Ignite dev@ignite.apache.org
VOLUME ["/opt/ignite-dev"]

# Set the timezone.
ENV TZ="/usr/share/zoneinfo/Europe/Moscow"

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# Set the ducker.creator label so that we know that this is a ducker image.  This will make it
# visible to 'ducker purge'.  The ducker.creator label also lets us know what UNIX user built this
# image.
ARG ducker_creator=default
LABEL ducker.creator=$ducker_creator

# Update Linux and install necessary utilities.
RUN apt update && apt install -y sudo netcat iptables rsync unzip wget curl jq coreutils openssh-server net-tools vim python-pip python-dev libffi-dev libssl-dev cmake pkg-config libfuse-dev iperf traceroute mc && apt-get -y clean
RUN python -m pip install -U pip==9.0.3;
RUN pip install --upgrade cffi virtualenv pyasn1 boto3 pycrypto pywinrm ipaddress enum34 && pip install --upgrade ducktape==0.7.7

# Set up ssh
COPY ./ssh-config /root/.ssh/config
# NOTE: The paramiko library supports the PEM-format private key, but does not support the RFC4716 format.
RUN ssh-keygen -m PEM -q -t rsa -N '' -f /root/.ssh/id_rsa && cp -f /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
RUN echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config

ARG APACHE_MIRROR="https://apache-mirror.rbc.ru/pub/apache/"
ARG APACHE_ARCHIVE="https://archive.apache.org/dist/"

# Install binary test dependencies.
RUN cd /opt && curl -O $APACHE_ARCHIVE/ignite/2.7.6/apache-ignite-2.7.6-bin.zip && unzip apache-ignite-2.7.6-bin.zip && mv /opt/apache-ignite-2.7.6-bin /opt/ignite-2.7.6
RUN cd /opt && curl -O $APACHE_ARCHIVE/ignite/2.8.0/apache-ignite-2.8.0-bin.zip && unzip apache-ignite-2.8.0-bin.zip && mv /opt/apache-ignite-2.8.0-bin /opt/ignite-2.8.0
RUN cd /opt && curl -O $APACHE_ARCHIVE/ignite/2.8.1/apache-ignite-2.8.1-bin.zip && unzip apache-ignite-2.8.1-bin.zip && mv /opt/apache-ignite-2.8.1-bin /opt/ignite-2.8.1

RUN rm /opt/apache-ignite-*-bin.zip
RUN chmod a+wr /opt -R

# Install spark
ARG SPARK_VERSION="2.3.4"
ARG SPARK_NAME="spark-$SPARK_VERSION"
ARG SPARK_RELEASE_NAME="spark-$SPARK_VERSION-bin-hadoop2.7"

RUN cd /opt && curl -O $APACHE_MIRROR/spark/$SPARK_NAME/$SPARK_RELEASE_NAME.tgz && tar xvf $SPARK_RELEASE_NAME.tgz && rm $SPARK_RELEASE_NAME.tgz
RUN mv /opt/$SPARK_RELEASE_NAME /opt/$SPARK_NAME
RUN chmod a+wr /opt/$SPARK_NAME -R

# The version of Kibosh to use for testing.
# If you update this, also update vagrant/base.sh
ARG KIBOSH_VERSION="8841dd392e6fbf02986e2fb1f1ebf04df344b65a"

# Install Kibosh
RUN apt-get install fuse
RUN cd /opt && git clone -q  https://github.com/confluentinc/kibosh.git && cd "/opt/kibosh" && git reset --hard $KIBOSH_VERSION && mkdir "/opt/kibosh/build" && cd "/opt/kibosh/build" && ../configure && make -j 2

# Set up the ducker user.
RUN useradd -ms /bin/bash ducker && mkdir -p /home/ducker/ && rsync -aiq /root/.ssh/ /home/ducker/.ssh && chown -R ducker /home/ducker/ /mnt/ /var/log/ && echo "PATH=$(runuser -l ducker -c 'echo $PATH'):$JAVA_HOME/bin" >> /home/ducker/.ssh/environment && echo 'PATH=$PATH:'"$JAVA_HOME/bin" >> /home/ducker/.profile && echo 'ducker ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
USER ducker

CMD sudo service ssh start && tail -f /dev/null

# Container port exposure
EXPOSE 11211 47100 47500 49112 10800 8080