Apache Ignite .NET Docker module
===========================
Apache Ignite .NET Docker module provides Dockerfile and accompanying files for building docker image.


Build image
===========
1) Build Apache Ignite.NET as described at modules/platforms/dotnet/DEVNOTES.txt

2) Goto Apache Ignite's Docker module directory

        cd modules/docker

3) Copy Apache Ignite.NET's binaries to libs and publish folders

        cp -r ../../modules/platforms/dotnet/bin/libs libs
        cp -r ../../modules/platforms/dotnet/bin/netcoreapp2.0/publish/ publish

4) Build docker image

        docker build . -t apacheignite/ignite-net[:<version>]

   Prepared image will be available issuing `docker images` command
