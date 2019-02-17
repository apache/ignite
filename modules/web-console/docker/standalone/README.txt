Web Console Docker module
=========================
Web Console Docker module provides Dockerfile and accompanying files for building docker image.


Build image
===========
1) Build Apache Ignite binary archive as described in DEVNOTES.txt.

2) Goto Web Console's Docker module directory

        cd modules/web-console/docker/standalone

3) Copy build-related necessary files

        mkdir -pv build
        cp -rf ../../frontend ../../backend build
        cp -rfv ../../web-agent/target/ignite-web-agent-*.zip build/backend/agent_dists/

4) Build docker image

        docker build . -t apacheignite/web-console-standalone[:<version>]

   Prepared image will be available issuing `docker images` command
