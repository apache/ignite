Apache Ignite Calcite Testing Module
------------------------------------

Apache Ignite Calcite Testing module contains the tests that require both SQL engines, Calcite (ignite-calcite)
and H2 (ignite-indexing), on the classpath: cross-engine tests, engine configuration tests and tests that are
parameterized by the SQL engine. The tests of the ignite-calcite module itself run without the H2 engine.
