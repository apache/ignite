package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.LockLogSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.LockStackSnapshot;

public interface DumpProcessor {
    void processDump(LockLogSnapshot snapshot);
    void processDump(LockStackSnapshot snapshot);
    void processDump(ThreadDumpLocks snapshot);
}
