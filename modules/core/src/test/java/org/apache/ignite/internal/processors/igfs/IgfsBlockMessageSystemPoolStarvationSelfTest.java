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

package org.apache.ignite.internal.processors.igfs;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test to check for system pool starvation due to {@link IgfsBlocksMessage}.
 */
public class IgfsBlockMessageSystemPoolStarvationSelfTest extends IgfsCommonAbstractTest {
    /** First node name. */
    private static final String NODE_1_NAME = "node1";

    /** Second node name. */
    private static final String NODE_2_NAME = "node2";

    /** Data cache name. */
    private static final String DATA_CACHE_NAME = "data";

    /** Meta cache name. */
    private static final String META_CACHE_NAME = "meta";

    /** Key in data caceh we will use to reproduce the issue. */
    private static final Integer DATA_KEY = 1;

    /** First node. */
    private Ignite victim;

    /** Second node. */
    private Ignite attacker;

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Start nodes.
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

        victim = Ignition.start(config(NODE_1_NAME, ipFinder));
        attacker = Ignition.start(config(NODE_2_NAME, ipFinder));

        // Check if we selected victim correctly.
        if (F.eq(dataCache(victim).affinity().mapKeyToNode(DATA_KEY).id(), attacker.cluster().localNode().id())) {
            Ignite tmp = victim;

            victim = attacker;

            attacker = tmp;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(true);

        victim = null;
        attacker = null;

        super.afterTest();
    }

    /**
     * Test starvation.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testStarvation() throws Exception {
        // 1. Create two IGFS file to make all system threads busy.
        CountDownLatch fileWriteLatch = new CountDownLatch(1);

        final IgniteInternalFuture fileFut1 = createFileAsync(new IgfsPath("/file1"), fileWriteLatch);
        final IgniteInternalFuture fileFut2 = createFileAsync(new IgfsPath("/file2"), fileWriteLatch);

        // 2. Start transaction and keep it opened.
        final CountDownLatch txStartLatch = new CountDownLatch(1);
        final CountDownLatch txCommitLatch = new CountDownLatch(1);

        IgniteInternalFuture<Void> txFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                GridCacheAdapter dataCache = dataCache(attacker);

                try (IgniteInternalTx tx = dataCache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    dataCache.put(DATA_KEY, 0);

                    txStartLatch.countDown();

                    txCommitLatch.await();

                    tx.commit();
                }

                return null;
            }
        });

        txStartLatch.await();

        // 3. Start async operation to drain semaphore permits.
        final IgniteInternalFuture putFut = dataCache(victim).putAsync(DATA_KEY, 1);

        assert !awaitFuture(putFut);

        // 4. Write data to files and ensure we stuck.
        fileWriteLatch.countDown();

        assert !awaitFuture(fileFut1);
        assert !awaitFuture(fileFut2);

        // 5. Finish transaction.
        txCommitLatch.countDown();

        assert awaitFuture(txFut);

        // 6. Async put must succeed.
        assert awaitFuture(putFut);

        // 7. Writes must succeed.
        assert awaitFuture(fileFut1);
        assert awaitFuture(fileFut2);
    }

    /**
     * Await future completion.
     *
     * @param fut Future.
     * @return {@code True} if future completed.
     * @throws Exception If failed.
     */
    private static boolean awaitFuture(final IgniteInternalFuture fut) throws Exception {
        return GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                return fut.isDone();
            }
        }, 1000);
    }

    /**
     * Create IGFS file asynchronously.
     *
     * @param path Path.
     * @param writeStartLatch Write start latch.
     * @return Future.
     */
    private IgniteInternalFuture<Void> createFileAsync(final IgfsPath path, final CountDownLatch writeStartLatch) {
        return GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteFileSystem igfs = attacker.fileSystem(null);

                try (IgfsOutputStream out = igfs.create(path, true)) {
                    writeStartLatch.await();

                    out.write(new byte[1024]);

                    out.flush();
                }

                return null;
            }
        });
    }

    /**
     * Get data cache for node.
     *
     * @param node Node.
     * @return Data cache.
     * @throws Exception If failed.
     */
    private GridCacheAdapter dataCache(Ignite node) throws Exception  {
        return ((IgniteKernal)node).internalCache(DATA_CACHE_NAME);
    }

    /**
     * Create node configuration.
     *
     * @param name Node name.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration config(String name, TcpDiscoveryVmIpFinder ipFinder) throws Exception {
        // Data cache configuration.
        CacheConfiguration dataCcfg = new CacheConfiguration();

        dataCcfg.setName(DATA_CACHE_NAME);
        dataCcfg.setCacheMode(CacheMode.REPLICATED);
        dataCcfg.setAtomicityMode(TRANSACTIONAL);
        dataCcfg.setWriteSynchronizationMode(FULL_SYNC);
        dataCcfg.setAffinityMapper(new DummyAffinityMapper(1));
        dataCcfg.setMaxConcurrentAsyncOperations(1);

        // Meta cache configuration.
        CacheConfiguration metaCcfg = new CacheConfiguration();

        metaCcfg.setName(META_CACHE_NAME);
        metaCcfg.setCacheMode(CacheMode.REPLICATED);
        metaCcfg.setAtomicityMode(TRANSACTIONAL);
        metaCcfg.setWriteSynchronizationMode(FULL_SYNC);

        // File system configuration.
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDefaultMode(IgfsMode.PRIMARY);
        igfsCfg.setDataCacheName(DATA_CACHE_NAME);
        igfsCfg.setMetaCacheName(META_CACHE_NAME);
        igfsCfg.setFragmentizerEnabled(false);
        igfsCfg.setBlockSize(1024);

        // Ignite configuration.
        IgniteConfiguration cfg = getConfiguration(name);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCcfg, metaCcfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        cfg.setStripedPoolSize(0);
        cfg.setSystemThreadPoolSize(2);
        cfg.setRebalanceThreadPoolSize(1);
        cfg.setPublicThreadPoolSize(1);

        return cfg;
    }

    /**
     * Dimmy affinity mapper.
     */
    private static class DummyAffinityMapper extends IgfsGroupDataBlocksKeyMapper {
        /** */
        private static final long serialVersionUID = 0L;

        /** Dummy affinity key. */
        private static final Integer KEY = 1;

        /**
         * Constructor.
         *
         * @param grpSize Group size.
         */
        public DummyAffinityMapper(int grpSize) {
            super(grpSize);
        }

        /** {@inheritDoc} */
        @Override public Object affinityKey(Object key) {
            return KEY;
        }
    }
}
