Apache Ignite Internals
-----------------------

Contains implementation classes for Apache Ignite.

### Ignite Components

All internal Ignite components implements [GridComponent.java](GridComponent.java) - interface for
- [processors](processors) and for
- [managers](managers) - has associated SPI.

Service Provider Interface (SPI) abbreviation is here because Apache Ignite was designed as a pluggable product. But Ignite users usually do not define own implementations.

### Contexts
Ignite manages its components using Context used to access components and binding theirs to each other.
Component-related context in the Apache Ignite is an implementation of [GridKernalContext.java](GridKernalContext.java).
This context instance is usually referred in code as `kctx` or `ctx`.

Higher-level context, cache shared context is also defined in [processors/cache](processors/cache)

