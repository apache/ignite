Apache Ignite Binary API Module
------------------------

ignite-binary-api module is internal module to separate binary API and implementation.
Other modules like ignite-core must depend on ignite-binary-api, only.
Implementation of API in ignite-binary-impl, it added in runtime.

Note, class files of this module are copied in ignite-core.jar during project assembly
to ensure compatibility with previous Ignite releases.