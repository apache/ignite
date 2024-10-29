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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
    public void testTransactionalCacheNoWarn() throws Exception {
        checkCachesSnapshotCreationAndRestore(
            cacheConfiguration(CacheAtomicityMode.TRANSACTIONAL, 1, "cache3", null));
    }

    /** */
    @Test
    public void testDefaultCacheGroup() throws Exception {
        checkCachesSnapshotCreationAndRestore(prepareCacheConfs(null, null, null, null));
    }

    /** */
    @Test
    public void testMultipleCachesInGroupWarn() throws Exception {
        checkCachesSnapshotCreationAndRestore(prepareCacheConfs(null, "grp0", "grp0", null));
    }

    /** */
    private CacheConfiguration<Integer, Integer>[] prepareCacheConfs(String grp0, String grp1, String grp2, String grp3) {
        return new CacheConfiguration[] {
            cacheConfiguration(CacheAtomicityMode.ATOMIC, 0, "cache0", grp0),
            cacheConfiguration(CacheAtomicityMode.ATOMIC, 1, "cache1", grp1),
            cacheConfiguration(CacheAtomicityMode.ATOMIC, 1, "cache2", grp2),
            cacheConfiguration(CacheAtomicityMode.TRANSACTIONAL, 1, "cache3", grp3)
        };
    }

    /** */
    public void checkCachesSnapshotCreationAndRestore(CacheConfiguration<Integer, Integer>... ccfgs) throws Exception {
        List<Integer> allWarnCaches = new ArrayList<>();
        Map<String, List<Integer>> warnCachesByGrps = new HashMap<>();

        for (CacheConfiguration<?, ?> ccfg: ccfgs) {
            if (ccfg.getAtomicityMode() == CacheAtomicityMode.ATOMIC && ccfg.getBackups() > 0) {
                String grpName = ccfg.getGroupName() == null ? ccfg.getName() : ccfg.getGroupName();

                warnCachesByGrps.compute(grpName, (grp, caches) -> {
                    caches = caches == null ? new ArrayList<>() : caches;

                    int cacheNum = Integer.parseInt(ccfg.getName().replace("cache", ""));

                    caches.add(cacheNum);

                    allWarnCaches.add(cacheNum);

                    return caches;
                });
            }
        }

        checkWarnMessageOnCreateSnapshot(cachesPattern(allWarnCaches), ccfgs);
        checkWarnMessageOnRestoreSnapshot(cachesPattern(allWarnCaches), null);

        for (String grp: warnCachesByGrps.keySet())
            checkWarnMessageOnRestoreSnapshot(cachesPattern(warnCachesByGrps.get(grp)), F.asList(grp));
    }

    /** Transforms cache numbers to cache pattern. For example, [0, 1] -> cache[0,1], cache[0,1]. */
    private @Nullable String cachesPattern(List<Integer> cacheNums) {
        if (cacheNums.isEmpty())
            return null;

        return cacheNums.stream()
            .map(c -> "cache" + cacheNums)
            .collect(Collectors.joining(", "));
    }

    /** */
    private void checkWarnMessageOnCreateSnapshot(
        @Nullable String warnAtomicCaches,
        CacheConfiguration<Integer, Integer>... ccfgs
    ) throws Exception {
        this.ccfgs = ccfgs;

        Ignite g = startGrids(3);

        g.cluster().state(ClusterState.ACTIVE);

        for (CacheConfiguration<Integer, Integer> c: ccfgs) {
            for (int i = 0; i < 1_000; i++)
                g.cache(c.getName()).put(i, i);
        }

        LogListener lsnr = warnLogListener(warnAtomicCaches, 0);  // Should not warn for full snapshots.

        lsnLogger.registerListener(lsnr);

        g.snapshot().createSnapshot(SNP).get(getTestTimeout());

        assertTrue(warnAtomicCaches, lsnr.check());

        for (CacheConfiguration<Integer, Integer> c: ccfgs) {
            for (int i = 1_000; i < 2_000; i++)
                g.cache(c.getName()).put(i, i);
        }

        lsnr = warnLogListener(warnAtomicCaches, warnAtomicCaches == null ? 0 : 1);

        lsnLogger.registerListener(lsnr);

        g.snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        assertTrue(warnAtomicCaches, lsnr.check());
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

        LogListener lsnr = warnLogListener(warnAtomicCaches, 0);  // Should not warn for full snapshots.

        lsnLogger.registerListener(lsnr);

        g.snapshot().restoreSnapshot(SNP, restoreCacheGrps).get(getTestTimeout());

        assertTrue(warnAtomicCaches + " " + restoreCacheGrps, lsnr.check());

        g.destroyCaches(g.cacheNames());

        awaitPartitionMapExchange();

        lsnr = warnLogListener(warnAtomicCaches, warnAtomicCaches == null ? 0 : 1);

        lsnLogger.registerListener(lsnr);

        g.snapshot().restoreSnapshot(SNP, restoreCacheGrps, 1).get(getTestTimeout());

        assertTrue(warnAtomicCaches + " " + restoreCacheGrps, lsnr.check());
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
