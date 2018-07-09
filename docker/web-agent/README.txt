Apache Ignite Web Agent Docker module
=====================================
Apache Ignite Web Agent Docker module provides Dockerfile and accompanying files for building docker image of Web Agent.


Build image
===========
1) Build Apache Ignite Web Console module

        mvn clean install -T 2C \
                          -Pall-java,all-scala,licenses,web-console \
                          -pl :ignite-web-console -am \
                          -DskipTests

3) Copy Apache Ignite Web Agent's binary archive to Apache Ignite Web Console Docker module directory

        cp -rfv ../../modules/web-console/web-agent/target/ignite-web-agent-*.zip ./

4) Unpack and remove Apache Ignite Web Agent's binary archive

        unzip ignite-web-agent-*.zip

5) Build docker image

        docker build . -t apacheignite/web-agent[:<version>]

   Prepared image will be available in local docker registry (can be seen issuing `docker images` command)

