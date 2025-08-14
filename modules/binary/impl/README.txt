Apache Ignite Binary Implementation Module
------------------------

ignite-binary-impl module is internal module to separate binary API and implementation.
Other modules like ignite-core must not depend on ignite-binary-impl, and use ignite-binary-api.

Note, class files of this module are copied in ignite-core.jar during project assembly
to ensure compatibility with previous Ignite releases.