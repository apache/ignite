/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental;

import java.util.Collection;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class IncrementalSnapshotWarnAtomicCachesTest extends GridCommonAbstractTest {
    /** */
    private static final String SNP = "snapshot";

    /** */
    private static final ListeningTestLogger lsnLogger = new ListeningTestLogger();

    /** */
    private CacheConfiguration<Integer, Integer>[] ccfgs;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalCompactionEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setName("persistence")
                .setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(ccfgs);

        cfg.setGridLogger(lsnLogger);

        cfg.setConsistentId(String.valueOf(getTestIgniteInstanceIndex(instanceName)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testAtomicCacheWarn() throws Exception {
        checkWarnMessageOnCreateSnapshot("cache",
            cacheConfiguration(CacheAtomicityMode.ATOMIC, 1, "cache", "grp"));

        checkWarnMessageOnRestoreSnapshot("cache", null);
    }

    /** */
    @Test
    public void testAtomicCacheNoBackupsNoWarn() throws Exception {
        checkWarnMessageOnCreateSnapshot(null,
            cacheConfiguration(CacheAtomicityMode.ATOMIC, 0, "cache", "grp"));

        checkWarnMessageOnRestoreSnapshot(null, null);
    }

    /** */
    @Test
    public void testMultipleCachesSingleAtomicWarn() throws Exception {
        checkWarnMessageOnCreateSnapshot("cache0",
            cacheConfiguration(CacheAtomicityMode.ATOMIC, 1, "cache0", "grp0"),
            cacheConfiguration(CacheAtomicityMode.TRANSACTIONAL, 1, "cache1", "grp1"));

        checkWarnMessageOnRestoreSnapshot("cache0", null);
        checkWarnMessageOnRestoreSnapshot("cache0", F.asList("grp0"));
        checkWarnMessageOnRestoreSnapshot(null, F.asList("grp1"));
    }

    /** */
    @Test
    public void testCacheGroupWithAtomicWarn() throws Exception {
        checkWarnMessageOnCreateSnapshot("cache0",
            cacheConfiguration(CacheAtomicityMode.ATOMIC, 1, "cache0", "grp"),
            cacheConfiguration(CacheAtomicityMode.TRANSACTIONAL, 1, "cache1", "grp"));

        checkWarnMessageOnRestoreSnapshot("cache0", null);
    }

    /** */
    @Test
    public void testMultipleAtomicCachesWarn() throws Exception {
        checkWarnMessageOnCreateSnapshot("cache0, cache2",
            cacheConfiguration(CacheAtomicityMode.ATOMIC, 1, "cache0", "grp"),
            cacheConfiguration(CacheAtomicityMode.TRANSACTIONAL, 1, "cache1", "grp"),
            cacheConfiguration(CacheAtomicityMode.ATOMIC, 1, "cache2", "grp1"));

        checkWarnMessageOnRestoreSnapshot("cache[0,2], cache[0,2]", null);
        checkWarnMessageOnRestoreSnapshot("cache0", F.asList("grp"));
        checkWarnMessageOnRestoreSnapshot("cache2", F.asList("grp1"));
    }

    /** */
    @Test
    public void testTxCacheNoWarn() throws Exception {
        checkWarnMessageOnCreateSnapshot(null,
            cacheConfiguration(CacheAtomicityMode.TRANSACTIONAL, 1, "cache", "grp"));

        checkWarnMessageOnRestoreSnapshot(null, null);
    }

    /** */
    private void checkWarnMessageOnCreateSnapshot(
        String warnAtomicCaches,
        CacheConfiguration<Integer, Integer>... ccfgs
    ) throws Exception {
        this.ccfgs = ccfgs;

        Ignite g = startGrids(3);

        g.cluster().state(ClusterState.ACTIVE);

        for (CacheConfiguration<Integer, Integer> c: ccfgs) {
            for (int i = 0; i < 1_000; i++)
                g.cache(c.getName()).put(i, i);
        }

        LogListener lsnr = warnLogListener(warnAtomicCaches, 0);  // Should warn only for incremental caches.

        lsnLogger.registerListener(lsnr);

        g.snapshot().createSnapshot(SNP).get(getTestTimeout());

        assertTrue(lsnr.check());

        for (CacheConfiguration<Integer, Integer> c: ccfgs) {
            for (int i = 1_000; i < 2_000; i++)
                g.cache(c.getName()).put(i, i);
        }

        lsnr = warnLogListener(warnAtomicCaches, warnAtomicCaches == null ? 0 : 1);

        lsnLogger.registerListener(lsnr);

        g.snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        assertTrue(lsnr.check());
    }

    /** */
    private void checkWarnMessageOnRestoreSnapshot(
        @Nullable String warnAtomicCaches,
        @Nullable Collection<String> restoreCacheGrps
    ) throws Exception {
        stopAllGrids();

        cleanPersistenceDir(true);

        Ignite g = startGrids(3);

        g.cluster().state(ClusterState.ACTIVE);

        g.destroyCaches(g.cacheNames());

        awaitPartitionMapExchange();

        LogListener lsnr = warnLogListener(warnAtomicCaches, 0);  // Should warn only for incremental caches.

        lsnLogger.registerListener(lsnr);

        g.snapshot().restoreSnapshot(SNP, restoreCacheGrps).get(getTestTimeout());

        assertTrue(lsnr.check());

        g.destroyCaches(g.cacheNames());

        awaitPartitionMapExchange();

        lsnr = warnLogListener(warnAtomicCaches, warnAtomicCaches == null ? 0 : 1);

        lsnLogger.registerListener(lsnr);

        g.snapshot().restoreIncrementalSnapshot(SNP, restoreCacheGrps, 1).get(getTestTimeout());

        assertTrue(lsnr.check());
    }

    /** */
    private LogListener warnLogListener(@Nullable String atomicCaches, int times) {
        Pattern p = Pattern.compile(
            "Incremental snapshot \\[snpName=" + SNP + ", incIdx=1] contains ATOMIC caches with backups:"
            + (atomicCaches == null ? "" : " \\[" + atomicCaches) + ']');

        return LogListener.matches(p).times(times).build();
    }

    /** */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(CacheAtomicityMode mode, int backups, String name, String grpName) {
        return new CacheConfiguration<Integer, Integer>()
            .setName(name)
            .setGroupName(grpName)
            .setAtomicityMode(mode)
            .setBackups(backups)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(100));
    }
}
