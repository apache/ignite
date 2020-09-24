## Overview
The `ignitetest` framework provides basic functionality and services
to write integration tests for Apache Ignite. This framework bases on 
the `ducktape` test framework, for information about it check the links:
- https://github.com/confluentinc/ducktape - source code of the `ducktape`;
- http://ducktape-docs.readthedocs.io - documentation to the `ducktape`.

Structure of the `ignitetest` directory is:
- `./ignitetest/services` contains basic services functionality;
- `./ignitetest/utils` contains utils for testing;
- `./ignitetest/tests` contains tests.

Docker is used to emulate distributed environment. Single container represents 
a running node.

## Requirements
To just start tests locally the only requirement is preinstalled `docker`. 

For development process requirements are:
1. `python` >= 3.6;
2. `tox` (check python codestyle);
3. `docker`.
