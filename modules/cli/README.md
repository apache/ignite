# Ignite CLI module

Ignite CLI tool is a single entry point for any management operations.

## Build

    mvn package -f ../../pom.xml
## Run
For Windows:

    target/ignite.exe
For Linux/MacOS:

    ./target/ignite
## Examples
Download and prepare artifacts for run Ignite node:

    ignite init
Node start:

    ignite start consistent-id
Node stop:

    ignite stop consistent-id
Get current node configuration:

    ignite config get --node-endpoint=localhost:10300
Show help:

    ignite --help
    ignite init --help
    ...
