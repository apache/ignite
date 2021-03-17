# Apache Ignite Bytecode module
Fork of [PrestoDB Bytecode module (ver 0.243)](https://github.com/prestodb/presto/tree/0.243/presto-bytecode).
* Removed unnecessary guava dependency.
* Tests migrated from TestNG to JUnit 5.

This module provides a convenient thin wrapper around [ASM](https://asm.ow2.io/) library to generate classes at runtime.