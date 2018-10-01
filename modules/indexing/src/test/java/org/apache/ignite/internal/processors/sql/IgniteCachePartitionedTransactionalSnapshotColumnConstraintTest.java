package org.apache.ignite.internal.processors.sql;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.jetbrains.annotations.NotNull;

public class IgniteCachePartitionedTransactionalSnapshotColumnConstraintTest
    extends IgniteCachePartitionedAtomicColumnConstraintsTest {
    /** {@inheritDoc} */
    @NotNull @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
    }
}
