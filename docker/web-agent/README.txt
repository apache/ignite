Apache Ignite Web Agent Docker module
=====================================
Apache Ignite Web Agent Docker module provides Dockerfile and accompanying files
for building docker image of Web Agent.


Build image
===========
1) Build Apache Ignite Web Console module

        mvn clean install -T 2C \
                          -Pall-java,all-scala,licenses,web-console \
                          -pl :ignite-web-console -am \
                          -DskipTests

2) Go to Apache Ignite Web Console Docker module directory and copy Apache
   Ignite Web Agent's binary archive

        cd docker/web-agent
        cp -rfv ../../modules/web-console/web-agent/target/ignite-web-agent-*.zip ./

3) Unpack and remove Apache Ignite Web Agent's binary archive

        unzip ignite-web-agent-*.zip
        rm -rf ignite-web-agent-*.zip

4) Build docker image

        docker build . -t apacheignite/web-agent[:<version>]

   Prepared image will be available in local docker registry (can be seen
   issuing `docker images` command)

5) Clean up

        rm -rf ignite-web-agent*
