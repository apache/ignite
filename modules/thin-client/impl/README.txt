Apache Ignite Thin Client Implementation Module
------------------------

ignite-thin-client-impl module is internal module to separate thin-client API and implementation.
It contains the implementation of the thin client API declared in ignite-thin-client-api
(TcpIgniteClient and supporting classes).

Note, class files of this module are copied in ignite-core.jar during project assembly
to ensure compatibility with previous Ignite releases.
