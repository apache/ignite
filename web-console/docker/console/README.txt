Web Console Docker module
=====================================
Web Console Docker module provides Dockerfile and accompanying files for building docker image of Web Console.


Web Console Docker Image Build Instructions
=====================================
Install Docker (version >=17.05) using instructions from https://www.docker.com/community-edition.

1. To build Web Console All in One archive from sources run the following command in Ignite project root folder:
    mvn clean package \
        -Plgpl,web-console,release \
        -pl :ignite-web-console -am \
        -DskipTests -DskipClientDocs -Dmaven.javadoc.skip=true
        
    Assembled archive can be found here:  `modules/web-console/target/ignite-web-console-*.zip`.

2. Go to Web Console Docker module directory and copy Web Console All in One archive:

    cd modules/web-console/docker/console
    cp -rfv ../../target/ignite-web-console-*.zip ./

3. Unpack Web Console All in One binary archive

    unzip ignite-web-console-*.zip

4. Build backend docker image

    docker build . -f backend/Dockerfile -t gridgain/web-console-backend[:<version>]

5. Build frontend docker image

    docker build . -f frontend/Dockerfile -t gridgain/web-console-frontend[:<version>]

6. Clean up

    rm -rf ignite-web-console-*

The image will be available in local docker registry (can be seen by calling the `docker images` command)
    