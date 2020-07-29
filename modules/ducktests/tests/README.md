## Overview
The `ignitetest` framework provides basic functionality and services
to write integration tests for Apache Ignite. This framework bases on 
the `ducktape` test framework, for information about it check the links:
- https://github.com/confluentinc/ducktape - source code of the `ducktape`;
- http://ducktape-docs.readthedocs.io - documentation to the `ducktape`.

Structure of the `ignitetest` directory is:
- `./ignitetest/services` contains basic services functionality;
- `./ignitetest/tests/utils` contains utils for testing.  

## Review Checklist
1. All tests must be parameterized with `version`.
