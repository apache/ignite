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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests that reconciliation works with binary objects that are absent in the nodes classpath.
 */
@RunWith(Parameterized.class)
public class PartitionReconciliationBinaryObjectsTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    protected static final int NODES_CNT = 4;

    /** Keys count. */
    protected static final int KEYS_CNT = 100;

    /** Corrupted keys count. */
    protected static final int BROKEN_KEYS_CNT = 10;

    /** Custom key class. */
    private static final String CUSTOM_KEY_CLS = "org.apache.ignite.tests.p2p.ReconciliationCustomKey";

    /** Custom value class. */
    private static final String CUSTOM_VAL_CLS = "org.apache.ignite.tests.p2p.ReconciliationCustomValue";

    /** Cache atomicity mode. */
    @Parameterized.Parameter(0)
    public CacheAtomicityMode cacheAtomicityMode;

    /** Fix mode. */
    @Parameterized.Parameter(1)
    public boolean fixMode;

    /** Flag indicates additional classes should be included into the class-path. */
    private boolean useExtendedClasses;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(300L * 1024 * 1024))
        );

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(cacheAtomicityMode);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1));
        ccfg.setBackups(NODES_CNT - 1);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(name);

        cfg.setAutoActivationEnabled(false);

        if (useExtendedClasses)
            cfg.setClassLoader(getExternalClassLoader());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Parameterized.Parameters(name = "atomicity = {0}, fixMode = {1}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        CacheAtomicityMode[] atomicityModes = new CacheAtomicityMode[] {
            CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

        for (CacheAtomicityMode atomicityMode : atomicityModes) {
            params.add(new Object[] {atomicityMode, false});
            params.add(new Object[] {atomicityMode, true});
        }

        return params;
    }

    /**
     * Tests that reconciliation works with binary objects that are absent in the nodes classpath.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReconciliationOfColdKeysUnderLoad() throws Exception {
        useExtendedClasses = true;

        IgniteEx ig = startGrids(NODES_CNT);

        IgniteEx client = startClientGrid(NODES_CNT);

        ig.cluster().active(true);

        Class customKeyCls = ig.configuration().getClassLoader().loadClass(CUSTOM_KEY_CLS);
        Class customValCls = ig.configuration().getClassLoader().loadClass(CUSTOM_VAL_CLS);

        Constructor keyCtor = customKeyCls.getDeclaredConstructor(int.class);

        IgniteCache<Object, Object> clientCache = client.cache(DEFAULT_CACHE_NAME);

        Set<Integer> correctKeys = new HashSet<>();

        for (int i = 0; i < KEYS_CNT; i++) {
            clientCache.put(keyCtor.newInstance(i), customValCls.newInstance());
            correctKeys.add(i);
        }

        log.info(">>>> Initial data loading finished");

        int firstBrokenKey = KEYS_CNT / 2;

        GridCacheContext[] nodeCacheCtxs = new GridCacheContext[NODES_CNT];

        for (int i = 0; i < NODES_CNT; i++)
            nodeCacheCtxs[i] = grid(i).cachex(DEFAULT_CACHE_NAME).context();

        Set<Object> brokenKeys = new HashSet<>();

        for (int i = firstBrokenKey; i < firstBrokenKey + BROKEN_KEYS_CNT; i++) {
            Object brokenKey = keyCtor.newInstance(i);

            brokenKeys.add(brokenKey);

            if (i % 3 == 0)
                simulateMissingEntryCorruption(nodeCacheCtxs[i % NODES_CNT], brokenKey);
            else
                simulateOutdatedVersionCorruption(nodeCacheCtxs[i % NODES_CNT], brokenKey);
        }

        forceCheckpoint();

        log.info(">>>> Simulating data corruption finished");

        stopAllGrids();

        useExtendedClasses = false;

        ig = startGrids(NODES_CNT);

        ig.cluster().active(true);

        ReconciliationResult res = partitionReconciliation(ig, fixMode, RepairAlgorithm.PRIMARY, 4, DEFAULT_CACHE_NAME);

        log.info(">>>> Partition reconciliation finished");

        Set<PartitionReconciliationKeyMeta> conflictKeyMetas = conflictKeyMetas(res, DEFAULT_CACHE_NAME);

        assertEquals(BROKEN_KEYS_CNT, conflictKeyMetas.size());

        for (int i = firstBrokenKey; i < firstBrokenKey + BROKEN_KEYS_CNT; i++) {
            boolean keyMatched = false;

            for (PartitionReconciliationKeyMeta keyMeta : conflictKeyMetas) {
                if (keyMeta.stringView(true).contains("dummyField=" + String.valueOf(i)))
                    keyMatched = true;
            }

            assertTrue(
                "Unmatched key: " + i + ", got conflict key metas: " +
                    conflictKeyMetas.stream().map(m -> m.stringView(true)).reduce((s1, s2) -> s1 + ", " + s2).get(),
                keyMatched
            );
        }

    }
}
