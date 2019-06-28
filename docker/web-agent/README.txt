GridGain Web Agent Docker module
=====================================
GridGain Web Agent Docker module provides Dockerfile and accompanying files
for building docker image of Web Agent.


Build image
===========
1) Build GridGain Web Console Agent module

        mvn clean install \
                          -Pall-java,all-scala,licenses,web-console \
                          -pl :ignite-web-console-agent -am \
                          -DskipTests

2) Go to GridGain Web Console Docker module directory and copy GridGain Web Agent's binary archive

        cd docker/web-agent
        cp -rfv ../../modules/web-console/web-agent/target/ignite-web-console-agent-*.zip ./

3) Unpack and remove GridGain Web Agent's binary archive

        unzip ignite-web-console-agent-*.zip
        rm -rf ignite-web-console-agent-*.zip

4) Build docker image

        docker build . -t apacheignite/web-agent[:<version>]

5) Clean up

        rm -rf ignite-web-console-agent*

The image will be available in local docker registry (can be seen by calling the `docker images` command)