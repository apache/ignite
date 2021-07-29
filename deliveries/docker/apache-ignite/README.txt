Apache Ignite Docker module
===========================
Apache Ignite Docker module provides Dockerfile and accompanying files for building docker image.


Build image
===========
1) Build Apache Ignite binary archive as described in DEVNOTES.txt.

2) Goto Apache Ignite's Docker module directory

        cd docker/apache-ignite

3) Copy Apache Ignite's binary archive to Docker module directory

        cp -rfv ../../target/bin/apache-ignite-*.zip

4) Unpack Apache Ignite's binary archive

        unzip apache-ignite-*.zip

5) Build docker image

        docker build . -t apacheignite/ignite[:<version>]

   Prepared image will be available issuing `docker images` command
