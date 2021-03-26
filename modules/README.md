# Apache Ignite modules
We try to make Apache Ignite reasonably modular in order to simplify unit and integration testing.
Each module provides an exposed API which should be used by external modules. Not exposed APIs must not be used
by external modules. At the time of writing we do not use Java JIGSAW modules system, but it is likely that we will
at some point, which will help us to control the exported API contract.

We prohibit cyclic dependencies between modules in order to simplify JIGSAW migration in the future.

## Modules list

Module Name | Description
----------- | -----------
[api](api/README.md)|Ignite public API.
[bytecode](bytecode/README.md)|Ignite Bytecode module.
[cli](cli/README.md)|Ignite CLI implementation
[cli-common](cli-common/README.md)|Shared interfaces definitions for pluggable CLIng
[configuration](configuration/README.md)|Ignite configuration classes and configuration management framework
[configuration-annotation-processor](configuration-annotation-processor/README.md)|Tooling for generating Ignite configuration model classes from configuration schema definition
[network](network/README.md)|Networking module: group membership and message passi
[rest](rest/README.md)|REST management endpoint bindings and command handlers
[runner](runner/README.md)|Ignite server node runner. The module that wires up the Ignite components and handles node lifecycle.
[schema](schema/README.md)|Ignite schema API implementation and schema management classes.
[table](table/README.md)|Ignite table API implementation.