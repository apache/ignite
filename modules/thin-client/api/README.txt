Apache Ignite Binary API Module
------------------------

ignite-thin-client-api module is internal module to separate thin-client API and implementation.
Other modules like ignite-core must depend on ignite-thin-client-api, only.
Implementation of API in ignite-thin-client-impl, it added in runtime.

Note, class files of this module are copied in ignite-core.jar during project assembly
to ensure compatibility with previous Ignite releases.
