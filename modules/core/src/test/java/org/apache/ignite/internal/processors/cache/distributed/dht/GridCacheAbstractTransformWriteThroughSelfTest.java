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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheGenericTestStore;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Tests write-through.
 */
public abstract class GridCacheAbstractTransformWriteThroughSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    protected static final int GRID_CNT = 3;

    /** Update operation. */
    protected static final int OP_UPDATE = 0;

    /** Delete operation. */
    protected static final int OP_DELETE = 1;

    /** Near node constant. */
    protected static final int NEAR_NODE = 0;

    /** Primary node constant. */
    protected static final int PRIMARY_NODE = 0;

    /** Backup node constant. */
    protected static final int BACKUP_NODE = 0;

    /** Keys number. */
    public static final int KEYS_CNT = 30;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Value increment processor. */
    private static final EntryProcessor<String, Integer, Void> INCR_CLOS = new EntryProcessor<String, Integer, Void>() {
        @Override public Void process(MutableEntry<String, Integer> e, Object... args) {
            if (!e.exists())
                e.setValue(1);
            else
                e.setValue(e.getValue() + 1);

            return null;
        }
    };

    /** Value remove processor. */
    private static final EntryProcessor<String, Integer, Void> RMV_CLOS = new EntryProcessor<String, Integer, Void>() {
        @Override public Void process(MutableEntry<String, Integer> e, Object... args) {
            e.remove();

            return null;
        }
    };

    /** Test store. */
    private static List<GridCacheGenericTestStore<String, Integer>> stores =
        new ArrayList<>(GRID_CNT);

    /**
     * @return {@code True} if batch update is enabled.
     */
    protected abstract boolean batchUpdate();

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        GridCacheGenericTestStore<String, Integer> store = new GridCacheGenericTestStore<>();

        stores.add(store);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setCacheStoreFactory(singletonFactory(store));
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setNearConfiguration(new NearCacheConfiguration());

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        stores.clear();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (GridCacheGenericTestStore<String, Integer> store : stores)
            store.reset();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticNearUpdate() throws Exception {
        checkTransform(OPTIMISTIC, NEAR_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticPrimaryUpdate() throws Exception {
        checkTransform(OPTIMISTIC, PRIMARY_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticBackupUpdate() throws Exception {
        checkTransform(OPTIMISTIC, BACKUP_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticNearDelete() throws Exception {
        checkTransform(OPTIMISTIC, NEAR_NODE, OP_DELETE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticPrimaryDelete() throws Exception {
        checkTransform(OPTIMISTIC, PRIMARY_NODE, OP_DELETE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticBackupDelete() throws Exception {
        checkTransform(OPTIMISTIC, BACKUP_NODE, OP_DELETE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticNearUpdate() throws Exception {
        checkTransform(PESSIMISTIC, NEAR_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticPrimaryUpdate() throws Exception {
        checkTransform(PESSIMISTIC, PRIMARY_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticBackupUpdate() throws Exception {
        checkTransform(PESSIMISTIC, BACKUP_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticNearDelete() throws Exception {
        checkTransform(PESSIMISTIC, NEAR_NODE, OP_DELETE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticPrimaryDelete() throws Exception {
        checkTransform(PESSIMISTIC, PRIMARY_NODE, OP_DELETE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticBackupDelete() throws Exception {
        checkTransform(PESSIMISTIC, BACKUP_NODE, OP_DELETE);
    }

    /**
     * @param concurrency Concurrency.
     * @param nodeType Node type.
     * @param op Op.
     * @throws Exception If failed.
     */
    protected void checkTransform(TransactionConcurrency concurrency, int nodeType, int op) throws Exception {
        IgniteCache<String, Integer> cache = jcache(0);

        Collection<String> keys = keysForType(nodeType);

        for (String key : keys)
            cache.put(key, 1);

        GridCacheGenericTestStore<String, Integer> nearStore = stores.get(0);

        nearStore.reset();

        for (String key : keys)
            jcache(0).localClear(key);

        info(">>> Starting transform transaction");

        try (Transaction tx = ignite(0).transactions().txStart(concurrency, READ_COMMITTED)) {
            if (op == OP_UPDATE) {
                for (String key : keys)
                    cache.invoke(key, INCR_CLOS);
            }
            else {
                for (String key : keys)
                    cache.invoke(key, RMV_CLOS);
            }

            tx.commit();
        }

        if (batchUpdate()) {
            assertEquals(0, nearStore.getPutCount());
            assertEquals(0, nearStore.getRemoveCount());

            if (op == OP_UPDATE)
                assertEquals(1, nearStore.getPutAllCount());
            else
                assertEquals(1, nearStore.getRemoveAllCount());
        }
        else {
            assertEquals(0, nearStore.getPutAllCount());
            assertEquals(0, nearStore.getRemoveAllCount());

            if (op == OP_UPDATE)
                assertEquals(keys.size(), nearStore.getPutCount());
            else
                assertEquals(keys.size(), nearStore.getRemoveCount());
        }

        if (op == OP_UPDATE) {
            for (String key : keys)
                assertEquals((Integer)2, nearStore.getMap().get(key));
        }
        else {
            for (String key : keys)
                assertNull(nearStore.getMap().get(key));
        }
    }

    /**
     * @param nodeType Node type to generate keys for.
     * @return Collection of keys.
     */
    private Collection<String> keysForType(int nodeType) {
        Collection<String> keys = new ArrayList<>(KEYS_CNT);

        int numKey = 0;

        while (keys.size() < 30) {
            String key = String.valueOf(numKey);

            Affinity<Object> affinity = ignite(0).affinity(null);

            if (nodeType == NEAR_NODE) {
                if (!affinity.isPrimaryOrBackup(grid(0).localNode(), key))
                    keys.add(key);
            }
            else if (nodeType == PRIMARY_NODE) {
                if (affinity.isPrimary(grid(0).localNode(), key))
                    keys.add(key);
            }
            else if (nodeType == BACKUP_NODE) {
                if (affinity.isBackup(grid(0).localNode(), key))
                    keys.add(key);
            }

            numKey++;
        }

        return keys;
    }
}