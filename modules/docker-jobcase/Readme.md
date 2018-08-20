# Jobcase Apache Ignite Production Docker Image
 
## Introduction

The docker image includes the Apache Ignite binaries V2.5. The binaries are built from GitHub branch ignite-2.5 [link](https://github.com/percipiomedia/ignite.git).

The docker image follows the control pattern for running commands/scripts at container startup.

## History

* Initial version (5/21/2018): It includes Dockerfile using binaries from GitHub branch build, grid configuration files for multicast-, S3 bucket and file- Apache Ignite node discovery.

## TODO

* Document logging configuration.
* Add logic for setting setConsistentId with host DNS as suffix.
* Add remote debug support.
* Add JMX support.
* Document Xterm support.
* Add command for creating schema for binary objects from file ignite-schema.sql.
* Define and implement healthcheck logic.
* Add optional command for activating grid.
* Add optional command for updating baseline topology.

## Terminology

Docker Engine:
> *It is a client-server application with these major components:* 
> * *A server which is a type of long-running program called a daemon process (the dockerd command).* 
> * *A REST API which specifies interfaces that programs can use to talk to the daemon and instruct it what to do.* 
> * *A command line interface (CLI) client (the docker command).*

> *The daemon creates and manages Docker objects, such as images, containers, networks, and volumes.*

Docker Image: 
> *An image is an inert, immutable, file that’s essentially a snapshot of a container. Images are created with the build command, and they’ll produce a container when started with run.*

Docker Container:
> * A container is a runnable instance of an image. You can create, start, stop, move, or delete a container using the Docker API or CLI. You can connect a container to one or more networks, attach storage to it, or even create a new image based on its current state.*

## References

* [1]: https://apacheignite.readme.io/docs/rest-api/ "Apache Ignite REST-API"
* [2]: https://docs.docker.com/engine/tutorials/networkingcontainers/ "Networking Containers"
* [3]: https://docs.docker.com/network/overlay/ "Overlay Networks"
* [4]: https://luppeng.wordpress.com/2018/01/03/revisit-setting-up-an-overlay-network-on-docker-without-docker-swarm/ "Overlay Network Without Swarm"
* [5]: https://apacheignite.readme.io/docs/distributed-persistent-store "Apache Ignite Persistence"
* [6]: https://apacheignite.readme.io/docs/jvm-and-system-tuning#garbage-collection-tuning "Apache Ignite Garbage Collection"
* [7]: http://www.oracle.com/technetwork/articles/java/g1gc-1984535.html "Garbage First Garbage Collector Tuning"
* [8]: http://www.baeldung.com/jvm-garbage-collectors "JVM Garbage Collectors Overview"
* [9]: https://apacheignite.readme.io/docs/events "Apache Ignite Events"
* [10]: https://docs.docker.com/docker-for-mac/faqs/ "Docker for Mac FAQS"
* [11]: https://docs.docker.com/docker-for-mac/osxfs/ "Docker File System osxfs for Mac"
* [12]: https://docs.docker.com/docker-for-mac/networking/ "Docker Networking on Mac"
* [13]: https://github.com/mal/docker-for-mac-host-bridge "Docker for Mac - Host Bridge"
* [14]: https://apacheignite.readme.io/docs/binary-marshaller "Apache Ignite Binary Marshaller"

## Build

Copy Apache Ignite's binary archive into docker build context directory

~~~~
cp -rfv ../../target/bin/apache-ignite-fabric-*.zip
~~~~

Unpack Apache Ignite's binary archive

~~~~
unzip apache-ignite-fabric-*.zip
~~~~

Build docker image

~~~~
sudo docker build -t apacheignite/jobcase:2.5.0 .
~~~~

## Networking

The Docker Engine supports different types of networks (bridge, overlay and host).
The default network type is bridge.
When Docker gets installed, a default network with the name bridge is created.

List available docker networks:

~~~~
sudo docker network ls
~~~~

Inspect default network bridge:

~~~~
sudo docker network inspect bridge

[
    {
        "Name": "bridge",
        "Id": "71310d36d398903889325ea029e728fad578293e829ee99d3fbab71a563530f3",
        "Created": "2018-05-18T11:02:54.005764144Z",
        "Scope": "local",
        "Driver": "bridge",
        "EnableIPv6": false,
        "IPAM": {
            "Driver": "default",
            "Options": null,
            "Config": [
                {
                    "Subnet": "172.17.0.0/16",
                    "Gateway": "172.17.0.1"
                }
            ]
        },
        "Internal": false,
        "Attachable": false,
        "Ingress": false,
        "ConfigFrom": {
            "Network": ""
        },
        "ConfigOnly": false,
        "Containers": {},
        "Options": {
            "com.docker.network.bridge.default_bridge": "true",
            "com.docker.network.bridge.enable_icc": "true",
            "com.docker.network.bridge.enable_ip_masquerade": "true",
            "com.docker.network.bridge.host_binding_ipv4": "0.0.0.0",
            "com.docker.network.bridge.name": "docker0",
            "com.docker.network.driver.mtu": "1500"
        },
        "Labels": {}
    }
]
~~~~

### Bridge Networks

The Docker Engine creates a network interface docker0 for the bridge network on the host. It allows network communication between the host and the docker containers attached to the bridge network.
**It does not apply to Mac OS X docker hosts (see appendix).**

A bridge network is limited to containers within a single host running the Docker engine.

It is recommend to create your own bridge network:

~~~~
docker network create
  --subnet 172.19.0.0/16
  --gateway 172.19.0.1
  my-bridge
~~~~

Attach specific network to container:

~~~~
sudo docker run -it
    --net=my-bridge
    --name=ignite-percipio apacheignite/jobcase:2.4.0 
~~~~

When attaching a container to a bridge network and it should be reachable from the outside world, port mapping needs to be configured.

### Overlay Networks

A overlay network ([documentation link][3]) allows network communication between containers on separate Docker engine hosts. It is used for so called multi-host network communication. 

Command for defining overlay network (using Swarm and standalone containers):

~~~~
docker network create -d overlay --attachable --subnet=192.168.10.0/24 my-overlay
~~~~

Please refer to [link][4] for setting up overlay network without Swarm.

## Running Commands at Container Startup

The docker image makes use of `ENTRYPOINT`. It allows the container to run as executable.

~~~~
ENTRYPOINT ["/bin/bash", "entrypoint.sh"]

# Set default arguments for ENTRYPOINT
CMD ["--debug", "--launch", "${IGNITE_HOME}/run.sh &"]
~~~~

The executable `entrypoint.sh` supports controller command pattern. You can pass-in a list of commands which should be executed at startup.

As default, it executes the Apache Ignite startup script `${IGNITE_HOME}/run.sh &` in background.

You can run additional commands at startup.

Example:

~~~~
sudo docker run -it
    --name=ignite-jobcase apacheignite/jobcase:2.4.0 
    "--launch ${IGNITE_HOME}/run.sh &;${IGNITE_HOME}/control.sh --activate"
~~~~

## Configuration

### Ports

Only one Ignite instance can run inside the docker container. The docker image does not define port ranges.

The default port values are:

~~~~
# Ports
ENV IGNITE_SERVER_PORT 11211
ENV IGNITE_JMX_PORT 49112
ENV IGNITE_DISCOVERY_PORT 47500
ENV IGNITE_COMMUNICATION_PORT 47100
ENV IGNITE_JDBC_PORT 10800
ENV IGNITE_REST_PORT 8080
~~~~

The defined port value(s) can be changed at container startup by passing environment variable.
E. g. `-e "IGNITE_DISCOVERY_PORT=48001"`. 


### Volumes

The docker image defines up-to four volumes.

~~~~
# JobCase home
ENV JOBCASE_HOME /opt/jobcase

# Location of configuration files
ENV JOBCASE_CONFIG ${JOBCASE_HOME}/config

# Location of container log files
ENV JOBCASE_LOGS ${JOBCASE_HOME}/logs

# root directory where Ignite will persist data, indexes and so on
ENV IGNITE_PERSISTENT_STORE ${JOBCASE_HOME}/db

# ip dicovery volume
ENV IGNITE_DISCOVERY /opt/jobcase/discovery
~~~~

If running the container locally on a development/test machine the log-, persistent- and discovery volumes should be mapped. E. g.

~~~~
sudo docker run -it
    -v /Users/mgay/ignite_nodes/logs:/opt/jobcase/logs
    -v /Users/mgay/ignite_nodes/db:/opt/jobcase/db
    -v /Users/mgay/ignite_nodes/discovery:/opt/jobcase/discovery
    --name=ignite-percipio apacheignite/percipiomedia:2.4.0 
~~~~

### Discovery

The docker image comes with Apache Ignite grid configuration files for three types of node discovery.

#### Multicast

As default, multicast is used for discovering other nodes in the grid.

The configuration file `/opt/jobcase/config/multicast.discovery.node.config.xml` defines the default settings.

#### File

Start container with Apache Ignite file discovery:

~~~~
    -e "CONFIG_URI=file:///opt/jobcase/config/file.discovery.node.config.xml"
~~~~


#### S3 Bucket

Start container using S3 bucket node discovery:

~~~~
    -e "CONFIG_URI=file:///opt/jobcase/config/s3bucket.discovery.node.config.xml"
~~~~

### Grid Persistence

The grid configuration file(s) enable native persistence.  Apache Ignite stores a superset of data on disk, and as much as it can in RAM based on the capacity of the latter. 

The persistence storage folder is located under `/opt/jobcase/data`. A data region called `Default_Region` is created with `[initSize=256.0 MiB, maxSize=2.0 GiB, persistenceEnabled=true]`.

As default, our grid configuration creates a unique id for the node by generating a UUID. And it is using the UUID to create a subdirectory under `/opt/jobcase/data`. In the subdirectory Apache Ignite persist the data for the node.

~~~~
        <!-- As a default, IGNITE creates a unique id for the node by generating a UUID. 
             It is used part of the persistent storage folder path. 
             E. g. persistent storage folder /opt/jobcase/db/node00-49755452-fa53-41d4-82d3-cfff0b830a57.
             
             It is recommended that the consistent id is set from outside IGNITE instead using generated value.
             
             If IGNITE_CONSISTENT_ID is not set, the custom version of  run.sh included within will force 
             it to a value of the form <prefix>_<uuid>, if a prefix can be determined.   The prefix will set to
             to the value of IGNITE_CONSISTENT_ID_PREFIX or if null to IGNITE_CLUSTER_NAME.
             
             If a prefix can be determined, if there is one directory in $IGNITE_PERSISTENT_STORE/store 
             which matches prefix_*, then directory name will be used as IGNITE_CONSISTENT_ID.   If no directories, 
             a new uuid will be generated.  If more than one match, that would be an error.
             
             The above allows different clusters with different names to co-exist on the same nodes, and allows 
             one cluster to be restored from a snapshot of a cluster of a different name.  Two clusters using the
             same prefix cannot exist on the same set of nodes.   
         -->
        <property name="consistentId" value="#{systemEnvironment['IGNITE_CONSISTENT_ID']}"/>
~~~~

Quote from Apache Ignite documentation [link][5]:
>To make sure a node gets assigned for a specific subdirectory and, thus, for specific data partitions even after restarts, set IgniteConfiguration.setConsistentId to a unique value cluster-wide. The consistent ID is mapped to UUID from node{IDX}-{UUID} string.

When starting the container, we can define a distinct consistent id value by passing an environment variable:

~~~~
    -e "IGNITE_CONSISTENT_ID=<mine_id_value>"
~~~~



### JVM Garbage Collection

As default, our grid configuration is using G1 garbage collector [link][6].

Default settings in Dockerfile:

~~~~
# Initial and maximum JVM Heap size
ENV JVM_HEAP_SIZE 1g

# JVM maximum amount of native memory that can be allocated for class metadata 
ENV JVM_METASPACE_SIZE 1g

# Ignite JVM settings
ENV JVM_OPTS="-XX:MaxMetaspaceSize=${JVM_METASPACE_SIZE} -server -Xms${JVM_HEAP_SIZE} -Xmx${JVM_HEAP_SIZE} -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:+ScavengeBeforeFullGC -XX:+DisableExplicitGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M -Xloggc:${JOBCASE_LOGS}/jvm-gc.log"
~~~~

The default JVM settings can be changed at container startup by passing environment variable `JVM_OPTS` and/or `JVM_HEAP_SIZE` and/or `JVM_METASPACE_SIZE`.

Setting memory values as equivalent in production environment:
 
~~~~
-e "JVM_METASPACE_SIZE=2g" -e "JVM_HEAP_SIZE=30g"
~~~~


### Events

Our grid configuration registers for a list of events. So it receives notification for the specified events happening in the cluster.

The selected events are so called low frequency events. To avoid performance issues as stated in Apache Ignite documentation (see below quote).

~~~~
       <property name="includeEventTypes">
          <list>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CHECKPOINT_SAVED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STARTED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_UNLOADED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_JOB_TIMEDOUT"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_JOB_FAILED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_JOB_FAILED_OVER"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_JOB_REJECTED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_JOB_CANCELLED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_TASK_TIMEDOUT"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_TASK_FAILED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CLASS_DEPLOY_FAILED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_TASK_DEPLOY_FAILED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_TASK_DEPLOYED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_TASK_UNDEPLOYED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STARTED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_NODE_JOINED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_NODE_LEFT"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_NODE_FAILED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CLASS_DEPLOYED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CLASS_UNDEPLOYED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CLASS_DEPLOY_FAILED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_TASK_DEPLOYED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_TASK_UNDEPLOYED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_TASK_DEPLOY_FAILED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CACHE_STARTED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CACHE_STOPPED"/>
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CACHE_NODES_LEFT"/>
          </list>
       </property>       
~~~~

Quote from Aache Ignite documentation:

> By default, event notifications are turned off for performance reasons.

> Since thousands of events per second are generated, it creates an additional load on the system. This can lead to significant performance degradation. Therefore, it is highly recommended to enable only those events that your application logic requires.

### Logging

TODO

### Ignite REST API

Apache Ignite supports REST protocol [link][1]. 

The API is accessible `http://<docker container ip>:8080/ignite?cmd=version`.
The default port value is `IGNITE_REST_PORT=80801`.

It can be overwritten by passing environment argument to the `docker run` command:

~~~~
    -e "IGNITE_REST_PORT=<port value>"
~~~~

## Appendix

### Mac OS X

The docker integration differs in several areas on Mac OS X. Some aspects are pointed out in the [link FAQS][10].

The Docker Engine starts a Linux VM on Mac OS X.

The below command allows to jump straight into the VM on a Mac.

~~~~
screen ~/Library/Containers/com.docker.docker/Data/com.docker.driver.amd64-linux/tty
~~~~

The Docker Engine for Mac uses a new file system called [link ‘osxfs’][11]. 

#### Networking

The Docker Engine does not create a network interface on Mac [link][12]. As a consequence, you  cannot ping docker containers on your macOS host. And you cannot access the containers by their ip-addresses on your macOS host.
You have to use port mapping for accessing docker container services on your macOS host.

You can install TUNTAP [link][13] to address the host access limitation. Or you are using port mapping in the association with `org.apache.ignite.configuration.BasicAddressResolver` in the grid configuration file.

When running Mlstore not inside a docker container instead on your macOS host. You have to map ports and define address resolvers if you did not install TUNTAP.

Add addressResolver to `<property name="discoverySpi">`. Replace the container ip-address `172.19.0.2` and macOS host address `192.168.49.117` with your environment values.

~~~~
                <property name="addressResolver">
                     <bean class="org.apache.ignite.configuration.BasicAddressResolver">
                         <constructor-arg>
                             <map>
                                 <entry key="172.19.0.2" value="192.168.49.117"/>
                             </map>
                         </constructor-arg>
                     </bean>
                </property>

~~~~

Add addressResolver to `<property name="communicationSpi">`. And update the ip-address values.

~~~~
                <property name="addressResolver">
                     <bean class="org.apache.ignite.configuration.BasicAddressResolver">
                         <constructor-arg>
                             <map>
                                 <entry key="172.19.0.2" value="192.168.49.117"/>
                             </map>
                         </constructor-arg>
                     </bean>
                </property>                                
~~~~

Launch the docker container with port mapping for discovery- and communication ports.
E. g. `-p 47100:47100 -p 47500:47500`.

#### Xterm Display

TODO