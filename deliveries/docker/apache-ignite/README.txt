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

6) Build ARM64 docker image

ARM64 image can be built on x64 hardware with `docker buildx`.

   - Install `docker buildx` plugin

        docker buildx install

   - Build and push ARM64 image (currently no way to push separately from build)

        docker buildx build . -f ./arm64/Dockerfile -t apacheignite/ignite:<version>-arm64 -t apacheignite/ignite:latest-arm64 --push --platform linux/arm64