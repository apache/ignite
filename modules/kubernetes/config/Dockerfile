# Use Java 8 image as default one.
FROM java:8

# Set Apache Ignite version.
ENV IGNITE_VERSION 2.0.0-SNAPSHOT

# Set IGNITE_HOME variable.
ENV IGNITE_HOME /opt/ignite/apache-ignite-fabric-lgpl-${IGNITE_VERSION}-bin

# Setting a path to an Apache Ignite configuration file. Used by run.sh script below.
ENV CONFIG_URI ${IGNITE_HOME}/config/example-kube.xml

# Make sure kubernetes lib is copied to 'libs' folder.
ENV OPTION_LIBS ignite-kubernetes

# Disabling quiet mode.
ENV IGNITE_QUIET=false

# Install or update needed tools.
RUN apt-get update && apt-get install -y --no-install-recommends unzip

# Creating and setting a working directory for following commands.
WORKDIR /opt/ignite

# Copying local Apache Ignite build to the docker image.
COPY ./apache-ignite-fabric-lgpl-${IGNITE_VERSION}-bin.zip apache-ignite-fabric-lgpl-${IGNITE_VERSION}-bin.zip

# Unpacking the build.
RUN unzip apache-ignite-fabric-lgpl-${IGNITE_VERSION}-bin.zip
RUN rm apache-ignite-fabric-lgpl-${IGNITE_VERSION}-bin.zip

# Copying the executable file and setting permissions.
COPY ./run.sh $IGNITE_HOME/
RUN chmod +x $IGNITE_HOME/run.sh

# Copying the configuration.
COPY ./example-kube.xml $IGNITE_HOME/config

# Starting an Apache Ignite node.
CMD $IGNITE_HOME/run.sh

# Exposing the ports.
EXPOSE 11211 47100 47500 49112


