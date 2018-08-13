Apache Ignite Web Console Standalone Docker module
=====================================
Apache Ignite Web Console Standalone Docker module provides Dockerfile and accompanying files
for building docker image of Web Console.


Ignite Web Console Build Instructions
=====================================
1. Build ignite-web-agent module follow instructions from 'modules/web-console/web-agent/README.txt'.
2. Copy ignite-web-agent-<version>.zip from 'modules/web-console/web-agent/target'
 to 'docker/web-console/standalone' folder.

Build image
===========
1) Go to Apache Ignite Web Console Docker module directory and copy Apache
   Ignite Web Console's frontend and backend folders

        cd docker/web-console/standalone
        cp -rfv ../../../modules/web-console/backend ./
        cp -rfv ../../../modules/web-console/frontend/build ./

2) Build docker image

        docker build . -t apacheignite/web-console-standalone:[:<version>]

   Prepared image will be available in local docker registry (can be seen
   issuing `docker images` command)

3) Clean up

        rm -rf backend
        rm -rf build
        rm -rf ignite-web-agent*