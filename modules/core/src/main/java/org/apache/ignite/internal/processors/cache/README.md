GridGain Cache Processors
------------------------------
### Context
[GridCacheSharedContext.java](GridCacheSharedContext.java) is a class which is used for binding cache-related managers.
This context is shared by all caches defined. This context instance is usually referred in code as `sctx`, `cctx` or `ctx`.

### Native Persistence
GridGain has its own [Native Persistence](persistence) - Implementation

### GridGain Cache Entries
Each entry represended by a subclass [GridCacheMapEntry](GridCacheMapEntry.java)