/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/** */
public class CacheOpsConcurrentDDLSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Create common node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(128 * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        return optimize(cfg);
    }

    /**
     * Create server node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    IgniteConfiguration serverConfiguration(int idx) throws Exception {
        return commonConfiguration(idx);
    }

    /**
     * @param idx Node index.
     * @return Client configuration.
     * @throws Exception if failed.
     */
    protected IgniteConfiguration clientConfiguration(int idx) throws Exception {
        return commonConfiguration(idx).setClientMode(true);
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDDLWaitsTxCompletion() throws Exception {
        startNode(serverConfiguration(1));
        startNode(serverConfiguration(2));
        startNode(serverConfiguration(3));
        startNode(clientConfiguration(4));

        waitForDiscovery(grid(1), grid(2), grid(3), grid(4));

        runSql(grid(4), "CREATE TABLE test(id INT PRIMARY KEY, a CHAR) WITH \"atomicity=transactional\"");

        final int k = 1;
        final int n = 4;

        try (Transaction tx = grid(n).transactions()
            .txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {

            IgniteCache<Integer, String> cache = grid(n).cache("SQL_PUBLIC_TEST").withKeepBinary();

            cache.put(k, "1");

            cache.put(k, cache.get(k));

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    runSql(grid(n), "ALTER TABLE test ADD COLUMN b TINYINT");
                }
            });

            tx.commit();

            fut.get();
        }
    }

    /**
     * tx.addActiveCache waits for finishDdl
     *
     * @throws Exception if failed.
     */
    public void testTwoCacheTxWaitDDLCompletion() throws Exception {
        startNode(serverConfiguration(1));
        startNode(serverConfiguration(2));
        startNode(serverConfiguration(3));
        startNode(clientConfiguration(4));

        waitForDiscovery(grid(1), grid(2), grid(3), grid(4));

        runSql(grid(4), "CREATE TABLE test(id INT PRIMARY KEY, a CHAR) WITH \"atomicity=transactional\"");
        runSql(grid(4), "CREATE TABLE test2(id INT PRIMARY KEY, a CHAR) WITH \"atomicity=transactional\"");

        final int k = 1;
        final int n = 3;

        try (Transaction tx = grid(n).transactions()
            .txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {

            final IgniteCache<Integer, String> cache = grid(n).cache("SQL_PUBLIC_TEST").withKeepBinary();

            cache.put(k, "1");

            final IgniteCache<Integer, String> cache2 = grid(n).cache("SQL_PUBLIC_TEST2").withKeepBinary();

            final CountDownLatch latch = new CountDownLatch(5);

            for (int i = 1; i <= 4; i++)
                ((BlockingIndexing)grid(i).context().query().getIndexing()).latch = latch;

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    runSql(grid(n), "ALTER TABLE test2 ADD COLUMN b TINYINT");
                }
            });

            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return latch.getCount() == 1;
                }
            }, 5000));

            latch.countDown();

            cache2.put(k, cache.get(k));

            fut.get();

            tx.commit();
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testGetWaitsForDDL() throws Exception {
        startNode(serverConfiguration(1));
        startNode(serverConfiguration(2));
        startNode(serverConfiguration(3));
        startNode(clientConfiguration(4));

        waitForDiscovery(grid(1), grid(2), grid(3), grid(4));

        runSql(grid(4), "CREATE TABLE test(id INT PRIMARY KEY, a CHAR) WITH \"atomicity=atomic\"");

        final int k = 3;
        final int n = 4;

        IgniteCache<Integer, String> cache = grid(n).cache("SQL_PUBLIC_TEST").withKeepBinary();

        cache.put(k, "test");

        final CountDownLatch latch = new CountDownLatch(5);

        for (int i = 1; i <= 4; i++)
            ((BlockingIndexing)grid(i).context().query().getIndexing()).latch = latch;

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                runSql(grid(n), "ALTER TABLE test ADD COLUMN b TINYINT");
            }
        });

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return latch.getCount() == 1;
            }
        }, 5000));

        IgniteFuture getFut = cache.getAllAsync(new HashSet(F.asList(1, 2, 3, 4)));

        latch.countDown();

        fut.get();

        getFut.get();
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDDLWaitsForGet() throws Exception {
        startNode(serverConfiguration(1));
        startNode(serverConfiguration(2));
        startNode(serverConfiguration(3));
        startNode(clientConfiguration(4));

        waitForDiscovery(grid(1), grid(2), grid(3), grid(4));

        runSql(grid(4), "CREATE TABLE test(id INT PRIMARY KEY, a CHAR) WITH \"atomicity=atomic\"");

        final int k = 3;
        final int n = 3;

        IgniteCache<Integer, String> cache = grid(n).cache("SQL_PUBLIC_TEST").withKeepBinary();

        cache.put(k, "test");

        blockMessage(GridNearGetResponse.class);

        IgniteFuture<?> getFut = cache.getAllAsync(new HashSet(F.asList(1, 2, 3, 4)));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return 2 == getBlockedMsgCount();
            }
        }, 5000));

        final CountDownLatch latch = new CountDownLatch(5);

        for (int i = 1; i <= 4; i++)
            ((BlockingIndexing)grid(i).context().query().getIndexing()).latch = latch;

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                runSql(grid(n), "ALTER TABLE test ADD COLUMN b TINYINT");
            }
        });

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return latch.getCount() == 1;
            }
        }, 5000));

        latch.countDown();

        unblockMessage();

        getFut.get();

        fut.get();
    }

    /**
     * Spoof blocking indexing class and start new node.
     * @param cfg Node configuration.
     * @return New node.
     */
    private static IgniteEx startNode(IgniteConfiguration cfg) {
        // Have to do this for each starting node - see GridQueryProcessor ctor, it nulls
        // idxCls static field on each call.
        GridQueryProcessor.idxCls = BlockingIndexing.class;

        IgniteEx node = (IgniteEx)Ignition.start(cfg);

        return node;
    }

    /**
     * Blocks message sending.
     *
     * @param cls
     */
    private void blockMessage(Class<?> cls) {
        for (int i = 1; i <= 4; i++) {
            IgniteEx node = grid(i);

            ((TestCommunicationSpi)node.configuration().getCommunicationSpi()).blockMessages(cls);
        }
    }

    /**
     * Sends blocked messages.
     */
    private void unblockMessage() {
        for (int i = 1; i <= 4; i++)
            ((TestCommunicationSpi)grid(i).configuration().getCommunicationSpi()).stopBlock(true);
    }

    /**
     *
     * @return number of blocked messages.
     */
    private int getBlockedMsgCount() {
        int n = 0;

        for (int i = 1; i <= 4; i++)
            n += ((TestCommunicationSpi)grid(i).configuration().getCommunicationSpi()).blockCount();

        return n;
    }

    /**
     *
     * @param node
     * @param sql
     */
    public void runSql(IgniteEx node, String sql) {
        node.context().query().querySqlFieldsNoCache(new SqlFieldsQuery(sql), true).getAll();
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private List<T2<ClusterNode, GridIoMessage>> blockedMsgs = new ArrayList<>();

        /** */
        private Set<Class<?>> blockCls = new HashSet<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            synchronized (this) {
                if (msg instanceof GridIoMessage) {
                    Object msg0 = ((GridIoMessage)msg).message();

                    if (blockCls.contains(msg0.getClass())) {
                        log.info("Block message [node=" +
                            node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME) + ", msg=" + msg0 + ']');

                        blockedMsgs.add(new T2<>(node, (GridIoMessage)msg));

                        return;
                    }
                }
            }

            super.sendMessage(node, msg, ackClosure);
        }

        /**
         * @param cls Message class.
         */
        void blockMessages(Class<?> cls) {
            synchronized (this) {
                blockCls.add(cls);
            }
        }

        /**
         * @param snd Send messages flag.
         */
        void stopBlock(boolean snd) {
            synchronized (this) {
                blockCls.clear();

                if (snd) {
                    for (T2<ClusterNode, GridIoMessage> msg : blockedMsgs) {
                        ClusterNode node = msg.get1();

                        log.info("Send blocked message: [node=" +
                            node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME) +
                            ", msg=" + msg.get2().message() + ']');

                        super.sendMessage(msg.get1(), msg.get2());
                    }
                }

                blockedMsgs.clear();
            }
        }

        /**
         *
         * @return number of blocked messages.
         */
        public int blockCount() {
            return blockedMsgs.size();
        }
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** */
        public volatile CountDownLatch latch;

        /** {@inheritDoc} */
        @Override public void onBeforeSchemaChange() {
            if (latch != null) {
                try {
                    latch.countDown();
                    latch.await();
                }
                catch (InterruptedException e) {
                    // No-op
                }
            }

            super.onBeforeSchemaChange();
        }
    }

}
