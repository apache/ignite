Apache Ignite Web Console Standalone Docker module
==================================================
Apache Ignite Web Console Standalone Docker module provides Dockerfile and accompanying files
for building docker image of Web Console.


Ignite Web Console Standalone Docker Image Build Instructions
=============================================================
1) Build ignite-web-console module

        mvn clean install -P web-console -DskipTests -T 2C -pl :ignite-web-console -am

2) Copy ignite-web-agent-<version>.zip from 'modules/web-console/web-agent/target'
   to 'docker/web-console/standalone' directory

        cp -rf modules/web-console/web-agent/target/ignite-web-agent-*.zip docker/web-console/standalone

3) Go to Apache Ignite Web Console Docker module directory and copy Apache
   Ignite Web Console's frontend and backend directory

        cd docker/web-console/standalone
        cp -rf ../../../modules/web-console/backend ./
        cp -rf ../../../modules/web-console/frontend ./

4) Build docker image

        docker build . -t apacheignite/web-console-standalone:[:<version>]

   Prepared image will be available in local docker registry (can be seen
   issuing `docker images` command)

5) Clean up

        rm -rf backend frontend ignite-web-agent*

