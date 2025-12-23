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

ARG jdk_version=eclipse-temurin:17
FROM $jdk_version

MAINTAINER Apache Ignite dev@ignite.apache.org
VOLUME ["/opt/ignite-dev"]

# ---------------------------------------------------------------------
# 1. Environment & Global System Configurations
# ---------------------------------------------------------------------
ENV TZ=Europe/Moscow
ENV DEBIAN_FRONTEND=noninteractive

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Set up ducker labels
ARG ducker_creator=default
LABEL ducker.creator=$ducker_creator

# Fix package mirror protocol from http to https
RUN cat /etc/apt/sources.list | sed 's/http:\/\/deb.debian.org/https:\/\/deb.debian.org/g' > /etc/apt/sources.list.2 && mv /etc/apt/sources.list.2 /etc/apt/sources.list

# ---------------------------------------------------------------------
# 2. System Utilities & Python 3.12 Installation
# ---------------------------------------------------------------------
RUN apt update && apt install -y software-properties-common gpg curl wget unzip rsync sudo \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt update && apt install -y \
    sudo \
    netcat-traditional \
    iptables \
    rsync \
    unzip \
    wget \
    curl \
    jq \
    coreutils \
    openssh-server \
    net-tools \
    vim \
    python3.12 \
    python3.12-dev \
    python3.12-venv \
    libffi-dev \
    libssl-dev \
    cmake \
    pkg-config \
    libfuse-dev \
    iperf \
    traceroute \
    mc \
    git \
    && apt-get -y clean

# ---------------------------------------------------------------------
# 3. Python Virtual Environment Setup & Requirements
# ---------------------------------------------------------------------
# Enforce Python 3.12 for our internal virtual environment
RUN python3.12 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Print the active python version to the build terminal to confirm 3.12 is loaded
RUN python3 --version

COPY ./requirements.txt /root/requirements.txt
RUN pip3 install --no-cache-dir -r /root/requirements.txt

# ---------------------------------------------------------------------
# 4. SSH & Security Settings
# ---------------------------------------------------------------------
COPY ./ssh-config /root/.ssh/config
RUN ssh-keygen -m PEM -q -t rsa -N '' -f /root/.ssh/id_rsa && cp -f /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
RUN echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config

# ---------------------------------------------------------------------
# 5. Apache Component Stack Dependencies (Ignite, Zookeeper, Kafka, JMX)
# ---------------------------------------------------------------------
ARG APACHE_MIRROR="https://apache-mirror.rbc.ru/pub/apache/"
ARG APACHE_ARCHIVE="https://archive.apache.org/dist/"

# Install Ignite
RUN for v in "2.7.6" "2.17.0"; \
    do cd /opt; \
    curl -O $APACHE_ARCHIVE/ignite/$v/apache-ignite-$v-bin.zip;\
    unzip apache-ignite-$v-bin.zip && mv /opt/apache-ignite-$v-bin /opt/ignite-$v; \
    done \
    && rm /opt/apache-ignite-*-bin.zip

# Install Zookeeper
ARG ZOOKEEPER_VERSION="3.5.8"
ARG ZOOKEEPER_NAME="zookeeper-$ZOOKEEPER_VERSION"
ARG ZOOKEEPER_RELEASE_NAME="apache-$ZOOKEEPER_NAME-bin"
ARG ZOOKEEPER_RELEASE_ARTIFACT="$ZOOKEEPER_RELEASE_NAME.tar.gz"
RUN cd /opt && curl -O $APACHE_ARCHIVE/zookeeper/$ZOOKEEPER_NAME/$ZOOKEEPER_RELEASE_ARTIFACT \
    && tar xvf $ZOOKEEPER_RELEASE_ARTIFACT && rm $ZOOKEEPER_RELEASE_ARTIFACT \
    && mv /opt/$ZOOKEEPER_RELEASE_NAME /opt/$ZOOKEEPER_NAME

# Install Kafka
ARG KAFKA_VERSION="3.9.1"
ARG KAFKA_NAME="kafka"
ARG KAFKA_RELEASE_NAME="${KAFKA_NAME}_2.13-$KAFKA_VERSION"
ARG KAFKA_RELEASE_ARTIFACT="$KAFKA_RELEASE_NAME.tgz"
RUN cd /opt && curl -O $APACHE_ARCHIVE/kafka/$KAFKA_VERSION/$KAFKA_RELEASE_ARTIFACT \
    && tar xvf $KAFKA_RELEASE_ARTIFACT && rm $KAFKA_RELEASE_ARTIFACT \
    && mv /opt/$KAFKA_RELEASE_NAME /opt/$KAFKA_NAME-$KAFKA_VERSION

# Install Jmxterm
ARG JMXTERM_NAME="jmxterm"
ARG JMXTERM_VERSION="1.0.1"
ARG JMXTERM_ARTIFACT="$JMXTERM_NAME-$JMXTERM_VERSION-uber.jar"
RUN cd /opt && curl -OL https://github.com/jiaqi/jmxterm/releases/download/v$JMXTERM_VERSION/$JMXTERM_ARTIFACT \
    && mv $JMXTERM_ARTIFACT $JMXTERM_NAME.jar

# ---------------------------------------------------------------------
# 6. User Isolation Permissions (Ducker Execution Account Setup)
# ---------------------------------------------------------------------
# Set up the ducker user.
ARG USER_UID=1000
ARG USER_GID=1000

RUN userdel -r ubuntu \
    && groupadd -g $USER_GID ducker \
    && useradd -u $USER_UID -g $USER_GID -ms /bin/bash ducker \
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

EXPOSE 11211 47100 47500 49112 10800 8080 2888 3888 2181 1098 8082 8083
