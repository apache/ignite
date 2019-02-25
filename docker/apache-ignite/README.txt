GridGain Docker module
===========================
GridGain Docker module provides Dockerfile and accompanying files for building docker image.


Build image
===========
1) Build GridGain binary archive as described in DEVNOTES.txt.

2) Goto GridGain's Docker module directory

        cd modules/docker

3) Copy GridGain's binary archive to Docker module directory

        cp -rfv ../../target/bin/apache-ignite-*.zip

4) Unpack GridGain's binary archive

        unzip apache-ignite-*.zip

5) Build docker image

        docker build . -t apacheignite/ignite[:<version>]

   Prepared image will be available issuing `docker images` command
