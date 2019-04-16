GridGain Internals
-----------------------

Contains implementation classes for GridGain.

### GridGain Components

All internal GridGain components implements [GridComponent.java](GridComponent.java) - interface for
- [processors](processors) and for
- [managers](managers) - has associated SPI.

Service Provider Interface (SPI) abbreviation is here because GridGain was designed as a pluggable product. But GridGain users usually do not define own implementations.

### Contexts
GridGain manages its components using Context used to access components and binding theirs to each other.
Component-related context in the GridGain is an implementation of [GridKernalContext.java](GridKernalContext.java).
This context instance is usually referred in code as `kctx` or `ctx`.

Higher-level context, cache shared context is also defined in [processors/cache](processors/cache)

