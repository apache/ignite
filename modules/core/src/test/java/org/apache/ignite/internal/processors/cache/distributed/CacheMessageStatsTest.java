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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * TODO: add number validation to test.
 */
public class CacheMessageStatsTest extends GridCommonAbstractTest {
    /** Grid count. */
    public static final int GRID_COUNT = 3;

    /** Caches count. */
    public static final int CACHES_CNT = 3;

    /** MB. */
    public static final long MB = 1024L * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        cfg.setCacheConfiguration(cacheConfigurations());

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true).setInitialSize(50 * MB).setMaxSize(50 * MB))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** */
    private CacheConfiguration[] cacheConfigurations() {
        return IntStream.range(0, CACHES_CNT).mapToObj(
            this::cacheConfiguration).collect(Collectors.toList()).toArray(new CacheConfiguration[CACHES_CNT]);
    }

    /**
     * @param idx Index.
     */
    protected CacheConfiguration cacheConfiguration(int idx) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME + idx);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(2);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setOnheapCacheEnabled(false);
        ccfg.setReadFromBackup(false);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_ENABLE_MESSAGE_STATS, "false");
        System.setProperty(IgniteSystemProperties.IGNITE_STAT_TOO_LONG_PROCESSING, "0");

        cleanPersistenceDir();

        startGridsMultiThreaded(GRID_COUNT);

        startGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IgniteSystemProperties.IGNITE_ENABLE_MESSAGE_STATS);
        System.clearProperty(IgniteSystemProperties.IGNITE_STAT_TOO_LONG_PROCESSING);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    public void testGetAfterTxPut() throws Exception {
        CacheConfiguration[] caches = cacheConfigurations();

        IgniteEx client = grid("client");

        for (int i = 0; i < 10; i++)
            grid(0).cache(caches[0].getName()).put(i, i);

        List<Integer> keys = primaryKeys(grid(0).cache(caches[0].getName()), 4);

        try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
            client.cache(caches[0].getName()).getAll(new HashSet<>(keys.subList(0, 2)));

            tx.commit();
        }
        catch (Exception e) {
            // No-op.
        }

        try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            client.cache(caches[0].getName()).getAll(new HashSet<>(keys.subList(2, 4)));

            tx.commit();
        }
        catch (Exception e) {
            // No-op.
        }

        client.cache(caches[0].getName()).get(keys.get(0));

        grid(0).context().io().dumpProcessedMessagesStats();

        client.cache(caches[0].getName()).get(keys.get(0));
        client.cache(caches[0].getName()).get(keys.get(1));

        grid(0).context().io().dumpProcessedMessagesStats();

        client.cache(caches[0].getName()).get(keys.get(0));
        client.cache(caches[1].getName()).get(keys.get(1));

        grid(0).context().io().dumpProcessedMessagesStats();

        for (int i = 0; i < 105; i++)
            client.cache(caches[0].getName()).get(keys.get(0));

        grid(0).context().io().dumpProcessedMessagesStats();

        Collection<Object> res = client.compute().broadcast(() -> null);

        Collection<Void> res2 = client.compute().broadcast(new Callable());

        grid(0).context().io().dumpProcessedMessagesStats();

        moreOperations();

        for (int i = 0; i < caches.length; i++)
            grid(0).cache(caches[i].getName()).removeAll();
    }

    protected void moreOperations() {
        // No-op.
    }

    private static class Callable implements IgniteCallable<Void> {
        @Override public Void call() throws Exception {
            return null;
        }
    }

}
