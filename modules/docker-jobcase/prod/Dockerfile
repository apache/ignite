# Base image for Jobcase Apache Ignite

#
# The image layout keeps same type of information in dedicated top-level directories. 
# E. g. all configuration information is in '/opt/jobcase/config'.
#
# Docker image layout:
#  |
#  - /opt/jobcase
#     |
#     | entrypoint.sh
#     |- logs
#     |- config
#     |- data
#     |- apache-ignite-fabric
#        |
#        | run.sh
#        |- bin
#        |- libs
#

# Debian package
FROM debian:stretch

# Ignite version
ENV IGNITE_VERSION 2.5.0

# Oracle JDK Linux x64 download information
ENV JAVA_VERSION=8 \
    JAVA_UPDATE=172 \
    JAVA_BUILD=11 \
    JAVA_PATH=a58eab1ec242421181065cdc37240b08
    
# Java home    
ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-oracle

# JobCase home
ENV JOBCASE_HOME /opt/jobcase

# Location of configuration files.
# # It includes logging configuration file
# and Apache Ignite grid configuration files 
# for common deployment scenarios. 
ENV JOBCASE_CONFIG ${JOBCASE_HOME}/config

# Location of container log files.
# It includes the entrypoint-, Apache Ignite- 
# and GC log files.
ENV JOBCASE_LOGS ${JOBCASE_HOME}/logs

# Ports
ENV IGNITE_SERVER_PORT 11211
ENV IGNITE_JMX_PORT 49112
ENV IGNITE_DISCOVERY_PORT 47500
ENV IGNITE_COMMUNICATION_PORT 47100
ENV IGNITE_JDBC_PORT 10800
ENV IGNITE_REST_PORT 8080

# verbose mode in Ignite
ENV IGNITE_QUIET false

# Default Ignite cluster definition file
ENV CONFIG_URI ${JOBCASE_CONFIG}/multicast.discovery.node.config.xml

# root directory where Ignite will persist data, indexes and so on
ENV IGNITE_PERSISTANT_STORE ${JOBCASE_HOME}/data

# ip dicovery volume for Shared File System Based Discovery
ENV IGNITE_DISCOVERY_FS ${JOBCASE_HOME}/discovery

# Amazon AWS S3 discovery bucket name
ENV IGNITE_DISCOVERY_S3_BUCKETNAME staging-mlstore-ignite-discovery-01

# Ignite home
# It includes the Apache Ignite startup script and binaries. 
ENV IGNITE_HOME ${JOBCASE_HOME}/apache-ignite-fabric

# Default deployment mode is CONTINUOUS
ENV IGNITE_DEPLOYMENT_MODE CONTINUOUS

# Initial and maximum JVM Heap size
ENV JVM_HEAP_SIZE 1g

# JVM maximum amount of native memory that can be allocated for class metadata 
ENV JVM_METASPACE_SIZE 1g

# Define additional JVM options for debug purpose
# As default, it enables remote debug on port 1044.
ENV JVM_DEBUG_OPTS "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=1044"

# Ignite JVM settings
ENV JVM_OPTS="-ea -XX:MaxMetaspaceSize=${JVM_METASPACE_SIZE} -server -Xms${JVM_HEAP_SIZE} -Xmx${JVM_HEAP_SIZE} -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:+ScavengeBeforeFullGC -XX:+DisableExplicitGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M -Xloggc:${JOBCASE_LOGS}/jvm-gc.log ${JVM_DEBUG_OPTS}"

# Do not rely on anything provided by base image(s), but be explicit, if they are installed already it is noop then
RUN apt-get update && apt-get install -y --no-install-recommends \
        unzip \
        curl \
        wget \
        ca-certificates \
        vim \
        procps \
        net-tools \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p ${JOBCASE_LOGS}

# Install Oracle JDK and Java Cryptography Extension (JCE) Unlimited Strength
RUN cd "/tmp" && \
    wget --header "Cookie: oraclelicense=accept-securebackup-cookie;" \
        "http://download.oracle.com/otn-pub/java/jdk/${JAVA_VERSION}u${JAVA_UPDATE}-b${JAVA_BUILD}/${JAVA_PATH}/jdk-${JAVA_VERSION}u${JAVA_UPDATE}-linux-x64.tar.gz" && \
    tar -xzf "jdk-${JAVA_VERSION}u${JAVA_UPDATE}-linux-x64.tar.gz" && \    
    mkdir -p "/usr/lib/jvm" && \
    mv "/tmp/jdk1.${JAVA_VERSION}.0_${JAVA_UPDATE}" "${JAVA_HOME}" && \
    wget --header "Cookie: oraclelicense=accept-securebackup-cookie;" \
        "http://download.oracle.com/otn-pub/java/jce/${JAVA_VERSION}/jce_policy-${JAVA_VERSION}.zip" && \
    unzip -jo -d "${JAVA_HOME}/jre/lib/security" "jce_policy-${JAVA_VERSION}.zip" && \
    rm /tmp/*
        
# Copy apache ignite binaries
COPY ./apache-ignite-fabric-${IGNITE_VERSION}-SNAPSHOT-bin ${IGNITE_HOME}/

# Enable log4j2
COPY ./apache-ignite-fabric-${IGNITE_VERSION}-SNAPSHOT-bin/libs/optional/ignite-log4j2 ${IGNITE_HOME}/libs/ignite-log4j2/

# Enable HTTP REST client
COPY ./apache-ignite-fabric-${IGNITE_VERSION}-SNAPSHOT-bin/libs/optional/ignite-rest-http ${IGNITE_HOME}/libs/ignite-rest-http/

# Enable AWS support
COPY ./apache-ignite-fabric-${IGNITE_VERSION}-SNAPSHOT-bin/libs/optional/ignite-aws ${IGNITE_HOME}/libs/ignite-aws/

# Copy log4j2 configuration file
COPY ./ignite-log4j2.xml ${JOBCASE_CONFIG}/

# Copy REST client configuration file
COPY ./jetty-config.xml ${JOBCASE_CONFIG}/

# Copy comnmon grid configuration file
COPY ./common.node.config.xml ${JOBCASE_CONFIG}/

# Copy comnmon grid configuration file
COPY ./file.discovery.node.config.xml ${JOBCASE_CONFIG}/

# Copy s3 bucket discovery grid configuration file
COPY ./s3bucket.discovery.node.config.xml ${JOBCASE_CONFIG}/

# Copy default grid configuration file
COPY ./multicast.discovery.node.config.xml ${JOBCASE_CONFIG}/

# Copy sh files and set permission
COPY ./run.sh ${IGNITE_HOME}/

RUN chmod +x ${IGNITE_HOME}/run.sh

# The entrypoint.sh script is always executed at container startup.
# It includes the logic for further runtime configuration and/or operations.
COPY ./entrypoint.sh ${JOBCASE_HOME}/
RUN chmod +x ${JOBCASE_HOME}/entrypoint.sh
COPY ./util.sh ${JOBCASE_HOME}/
RUN chmod +x ${JOBCASE_HOME}/util.sh

WORKDIR ${JOBCASE_HOME}

# The exec form is required for CMD providing default arguments 
# instead shell form 'ENTRYPOINT /bin/bash ${JOBCASE_HOME}/entrypoint.sh'.
# As a limitation, it will not do variable substitution like ${JOBCASE_HOME}.
# So we call 'WORKDIR ${JOBCASE_HOME}' prior.
ENTRYPOINT ["/bin/bash", "entrypoint.sh"]

# Set default arguments for ENTRYPOINT
CMD ["--debug", "--launch", "${IGNITE_HOME}/run.sh &"]

EXPOSE $IGNITE_SERVER_PORT $IGNITE_COMMUNICATION_PORT $IGNITE_DISCOVERY_PORT $IGNITE_JMX_PORT $IGNITE_JDBC_PORT $IGNITE_REST_PORT