package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/** */
public class IncrementalSnapshotTest extends AbstractSnapshotSelfTest {
    /** */
    @Test
    public void test() throws Exception {
        IgniteEx ign = startGridsWithCache(1, CACHE_KEYS_RANGE, key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        snp(ign).createSnapshot(SNAPSHOT_NAME).get();

        snp(ign).createIncrementalSnapshot(SNAPSHOT_NAME).get();
    }
}
