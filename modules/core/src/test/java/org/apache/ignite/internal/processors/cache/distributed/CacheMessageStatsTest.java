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

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * TODO: add number validation to test.
 */
public class CacheMessageStatsTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_COUNT = 3;

    /** Caches count. */
    private static final int CACHES_CNT = 3;

    /** MB. */
    private static final long MB = 1024L * 1024;

    /** */
    private static final int DEFAULT_ATOMIC_CACHE_INDEX = CACHES_CNT - 1;

    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        cfg.setCacheConfiguration(cacheConfigurations());

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true).setInitialSize(50 * MB).setMaxSize(50 * MB))
            .setPageSize(1024).setWalMode(WALMode.LOG_ONLY).setWalSegmentSize((int)(8 * MB));

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

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

        ccfg.setAtomicityMode(idx == DEFAULT_ATOMIC_CACHE_INDEX ? ATOMIC : TRANSACTIONAL);
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

        IgniteEx ex = startGrid(0);
        startGrid(1);
        startGrid(2);

        ex.cluster().active(true);

        awaitPartitionMapExchange();

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

        client.cache(caches[0].getName()).put(keys.get(0), 1);

        client.cache(caches[0].getName()).get(keys.get(0));

        grid(0).context().io().dumpProcessedMessagesStats();

        client.cache(caches[0].getName()).get(keys.get(0));
        client.cache(caches[0].getName()).get(keys.get(1));

        grid(0).context().io().dumpProcessedMessagesStats();

        client.cache(caches[0].getName()).get(keys.get(0));
        client.cache(caches[1].getName()).get(keys.get(1));

        grid(0).context().io().dumpProcessedMessagesStats();

        AtomicInteger cnt = new AtomicInteger();

        int getCnt = 105;

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                while (cnt.getAndIncrement() < getCnt)
                    client.cache(caches[0].getName()).get(keys.get(0));
            }
        }, Runtime.getRuntime().availableProcessors(), "get-thread");

        fut.get();

        // TODO FIXME Updates are logged after message is processed, so actual stats may lag behind cache API return.
        grid(0).context().io().dumpProcessedMessagesStats();

        Collection<Object> res = client.compute().broadcast(() -> null);

        Collection<Void> res2 = client.compute().broadcast(new Callable());

        grid(0).context().io().dumpProcessedMessagesStats();

        // atomics
        client.cache(caches[DEFAULT_ATOMIC_CACHE_INDEX].getName()).putAll(keys.stream().collect(Collectors.toMap(k -> k, k -> 0)));
        client.cache(caches[DEFAULT_ATOMIC_CACHE_INDEX].getName()).put(keys.get(0), 0);
        char[] res0 = client.cache(caches[DEFAULT_ATOMIC_CACHE_INDEX].getName()).invoke(keys.get(1), new MyInvoke());
        char[] res1 = client.cache(caches[DEFAULT_ATOMIC_CACHE_INDEX].getName()).invoke(keys.get(1), new MyInvoke());
        grid(0).context().io().dumpProcessedMessagesStats();

        moreOperations();
    }

    /** */
    protected void moreOperations() throws Exception {
        // No-op.
    }

    /** */
    private static class Callable implements IgniteCallable<Void> {
        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            return null;
        }
    }

    /** */
    private static class MyInvoke implements CacheEntryProcessor<Object, Object, char[]> {
        /** {@inheritDoc} */
        @Override public char[] process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
            Integer val0 = (Integer)entry.getValue();

            int val = val0 == null ? 0 : val0;

            entry.setValue(val + 1);

            return new char[] {(char) ((Integer)entry.getValue()).intValue()};
        }
    }

}
