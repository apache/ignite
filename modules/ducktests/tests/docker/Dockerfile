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

ARG jdk_version=eclipse-temurin:11
FROM $jdk_version

MAINTAINER Apache Ignite dev@ignite.apache.org
VOLUME ["/opt/ignite-dev"]

# Set the timezone.
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# Set the ducker.creator label so that we know that this is a ducker image.  This will make it
# visible to 'ducker purge'.  The ducker.creator label also lets us know what UNIX user built this
# image.
ARG ducker_creator=default
LABEL ducker.creator=$ducker_creator

# Update Linux and install necessary utilities.
RUN cat /etc/apt/sources.list | sed 's/http:\/\/deb.debian.org/https:\/\/deb.debian.org/g' > /etc/apt/sources.list.2 && mv /etc/apt/sources.list.2 /etc/apt/sources.list
RUN apt update && apt install -y sudo netcat-traditional iptables rsync unzip wget curl jq coreutils openssh-server net-tools vim python3-pip python3-dev python3-venv libffi-dev libssl-dev cmake pkg-config libfuse-dev iperf traceroute mc git && apt-get -y clean

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY ./requirements.txt /root/requirements.txt
RUN pip3 install -r /root/requirements.txt

# Set up ssh
COPY ./ssh-config /root/.ssh/config
# NOTE: The paramiko library supports the PEM-format private key, but does not support the RFC4716 format.
RUN ssh-keygen -m PEM -q -t rsa -N '' -f /root/.ssh/id_rsa && cp -f /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
RUN echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config

ARG APACHE_MIRROR="https://apache-mirror.rbc.ru/pub/apache/"
ARG APACHE_ARCHIVE="https://archive.apache.org/dist/"

# Install binary test dependencies.
RUN for v in "2.7.6" "2.16.0"; \
    do cd /opt; \
    curl -O $APACHE_ARCHIVE/ignite/$v/apache-ignite-$v-bin.zip;\
    unzip apache-ignite-$v-bin.zip && mv /opt/apache-ignite-$v-bin /opt/ignite-$v;\
    done

RUN rm /opt/apache-ignite-*-bin.zip

#Install zookeeper.
ARG ZOOKEEPER_VERSION="3.5.8"
ARG ZOOKEEPER_NAME="zookeeper-$ZOOKEEPER_VERSION"
ARG ZOOKEEPER_RELEASE_NAME="apache-$ZOOKEEPER_NAME-bin"
ARG ZOOKEEPER_RELEASE_ARTIFACT="$ZOOKEEPER_RELEASE_NAME.tar.gz"
RUN echo $APACHE_ARCHIVE/zookeeper/$ZOOKEEPER_NAME/$ZOOKEEPER_RELEASE_ARTIFACT
RUN cd /opt && curl -O $APACHE_ARCHIVE/zookeeper/$ZOOKEEPER_NAME/$ZOOKEEPER_RELEASE_ARTIFACT \
 && tar xvf $ZOOKEEPER_RELEASE_ARTIFACT && rm $ZOOKEEPER_RELEASE_ARTIFACT
RUN mv /opt/$ZOOKEEPER_RELEASE_NAME /opt/$ZOOKEEPER_NAME

# The version of Kibosh to use for testing.
# If you update this, also update vagrant/base.sh
ARG KIBOSH_VERSION="8841dd392e6fbf02986e2fb1f1ebf04df344b65a"

# Install Kibosh
RUN apt-get install fuse
RUN cd /opt && git clone -q  https://github.com/confluentinc/kibosh.git && cd "/opt/kibosh" && git reset --hard $KIBOSH_VERSION && mkdir "/opt/kibosh/build" && cd "/opt/kibosh/build" && ../configure && make -j 2

#Install jmxterm
ARG JMXTERM_NAME="jmxterm"
ARG JMXTERM_VERSION="1.0.1"
ARG JMXTERM_ARTIFACT="$JMXTERM_NAME-$JMXTERM_VERSION-uber.jar"
RUN cd /opt && curl -OL https://github.com/jiaqi/jmxterm/releases/download/v$JMXTERM_VERSION/$JMXTERM_ARTIFACT \
       && mv $JMXTERM_ARTIFACT $JMXTERM_NAME.jar

# Set up the ducker user.
RUN userdel -r ubuntu \
    && useradd -ms /bin/bash ducker \
    && mkdir -p /home/ducker/ \
    && rsync -aiq /root/.ssh/ /home/ducker/.ssh \
    && chown -R ducker /home/ducker/ /mnt/ /var/log/ /opt \
    && cp /dev/null /etc/environment \
    && echo "LANG=C.UTF-8" >> /home/ducker/.ssh/environment \
    && echo "PATH=$(runuser -l ducker -c 'echo $PATH'):$JAVA_HOME/bin:/opt/venv/bin" >> /home/ducker/.ssh/environment \
    && echo "JAVA_HOME=$JAVA_HOME" >> /home/ducker/.ssh/environment \
    && echo 'PATH=$PATH:'"$JAVA_HOME/bin" >> /home/ducker/.profile \
    && echo 'ducker ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
USER ducker

CMD sudo service ssh start && tail -f /dev/null

# Container port exposure
EXPOSE 11211 47100 47500 49112 10800 8080 2888 3888 2181 1098 8082
