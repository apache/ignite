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

package org.apache.ignite.internal.processors.cache.binary;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Latches;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Test scenario:
 *
 * <ul>
 *     <li>3 nodes in the grid</li>
 *     <li>client is connected to node with order=2</li>
 *     <li>client initiates update version=1</li>
 *     <li>Update is removed from node with order=3</li>
 *     <li>Tx is released and wait for missing update</li>
 *     <li>client starts new metadata change which contains previous update</li>
 * </ul>
 */
public class BinaryMetadataDelayedUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final int GRIDS = 3;

    /** */
    private static final int FIELDS = 2;

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private static final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        BlockTcpDiscoverySpi spi = new BlockTcpDiscoverySpi();
        spi.skipAddressesRandomization = true;

        cfg.setDiscoverySpi(spi.setIpFinder(ipFinder));

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        cfg.setMetricsUpdateFrequency(600_000);
        cfg.setClientFailureDetectionTimeout(600_000);
        cfg.setFailureDetectionTimeout(600_000);

//        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
//            setWalMode(LOG_ONLY).setPageSize(1024).setWalSegmentSize(16 * MB).
//            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(50 * MB)));

        QueryEntity qryEntity = new QueryEntity("java.lang.Integer", "Value");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        Collection<QueryIndex> indexes = new ArrayList<QueryIndex>(FIELDS);

        for (int i = 0; i < FIELDS; i++) {
            String name = "s" + i;

            fields.put(name, "java.lang.String");

            indexes.add(new QueryIndex(name, QueryIndexType.SORTED));
        }

        qryEntity.setFields(fields);

        qryEntity.setIndexes(indexes);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setBackups(0).
            setQueryEntities(Collections.singleton(qryEntity)).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
            setCacheMode(CacheMode.PARTITIONED));

        return cfg;
    }

    public void testMissingSchemaUpdate() throws Exception {
        // Start order is important.
        Ignite node0 = startGrid("node0");

        Ignite node1 = startGrid("node1");

        Ignite client0 = startGrid("client0");

        Ignite node2 = startGrid("node2");

        Ignite node3 = startGrid("node3");

        Ignite client1 = startGrid("client1");

        node0.cluster().active(true);

        awaitPartitionMapExchange();


        BlockTcpDiscoverySpi spi1 = (BlockTcpDiscoverySpi)node1.configuration().getDiscoverySpi();

        CountDownLatch srvL = new CountDownLatch(1);
        CountDownLatch clL = new CountDownLatch(1);

        AtomicBoolean clientWait = new AtomicBoolean();
        AtomicBoolean srvWait = new AtomicBoolean();

        final Object clientMux = new Object();
        final Object srvMux = new Object();

        spi1.setBlockPredicate(new IgniteBiPredicate<ClusterNode, DiscoveryCustomMessage>() {
            @Override public boolean apply(ClusterNode snd, DiscoveryCustomMessage msg) {
                if (msg instanceof MetadataUpdateProposedMessage) {
                    if (Thread.currentThread().getName().contains("client")) {
                        log.info("Block custom message to client: [locNode=" + snd + ", msg=" + msg + ']');

                        clL.countDown();

                        // Message to client
                        synchronized (clientMux) {
                            while (!clientWait.get())
                                try {
                                    clientMux.wait();
                                }
                                catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                        }

                    }
                    else {
                        log.info("Block custom message to server: [locNode=" + snd + ", msg=" + msg + ']');

                        srvL.countDown();

                        // Message to server
                        synchronized (srvMux) {
                            while (!srvWait.get())
                                try {
                                    srvMux.wait();
                                }
                                catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                        }
                    }

                    return true;
                }

                return false;
            }
        });

        Integer key = primaryKey(node2.cache(DEFAULT_CACHE_NAME));

        IgniteInternalFuture fut0 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = client1.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    client1.cache(DEFAULT_CACHE_NAME).put(key, build(client1, "val", 0));

                    tx.commit();
                }
                catch (Throwable t) {
                    log.error("err", t);
                }

            }
        });

        IgniteInternalFuture fut1 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                U.awaitQuiet(Latches.initMetaReq);

                // Await client block latch.
                U.awaitQuiet(clL);
//
//                Latches.lock = true;
//
//                Latches.l2.countDown();
//                U.awaitQuiet(Latches.l2);

                clientWait.set(true);

                synchronized (clientMux) {
                    clientMux.notify();
                }

                doSleep(3000);

                Latches.proposedClLock.countDown();
            }
        });

        IgniteInternalFuture fut2 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                Latches.lockT.set(true);

                try (Transaction tx = client0.transactions().
                    txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 0, 1)) {
                    client0.cache(DEFAULT_CACHE_NAME).put(key, build(client0, "val", 0));

                    tx.commit();
                }
                catch (Throwable t) {
                    log.error("err", t);
                }
            }
        });

        fut0.get();
        fut1.get();
        fut2.get();
    }

    protected BinaryObject build(Ignite ignite, String prefix, int... fields) {
        BinaryObjectBuilder builder = ignite.binary().builder("Value");

        for (int i = 0; i < fields.length; i++) {
            int field = fields[i];

            builder.setField("i" + field, field);
            builder.setField("s" + field, prefix + field);
        }

        return builder.build();
    }

    /**
     * Discovery SPI which can simulate network split.
     */
    protected class BlockTcpDiscoverySpi extends TcpDiscoverySpi {
        /** Block predicate. */
        private volatile IgniteBiPredicate<ClusterNode, DiscoveryCustomMessage> blockPred;

        /**
         * @param blockPred Block predicate.
         */
        public void setBlockPredicate(IgniteBiPredicate<ClusterNode, DiscoveryCustomMessage> blockPred) {
            this.blockPred = blockPred;
        }

        /**
         * @param addr Address.
         * @param msg Message.
         */
        private synchronized void block(ClusterNode addr, TcpDiscoveryAbstractMessage msg) {
            if (!(msg instanceof TcpDiscoveryCustomEventMessage))
                return;

            TcpDiscoveryCustomEventMessage cm = (TcpDiscoveryCustomEventMessage)msg;

            DiscoveryCustomMessage delegate;

            try {
                DiscoverySpiCustomMessage custMsg = cm.message(marshaller(), U.resolveClassLoader(ignite().configuration()));

                delegate = ((CustomMessageWrapper)custMsg).delegate();

            }
            catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            if (blockPred != null)
                blockPred.apply(addr, delegate);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            byte[] data,
            long timeout
        ) throws IOException {
            if (spiCtx != null)
                block(spiCtx.localNode(), msg);

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (spiCtx != null)
                block(spiCtx.localNode(), msg);

            //doSleep(500);

            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
//        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
//            long timeout) throws IOException {
//            if (spiCtx != null)
//                block(spiCtx.localNode(), msg);
//
//            //doSleep(500);
//
//            super.writeToSocket(msg, sock, res, timeout);
//        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }
}
