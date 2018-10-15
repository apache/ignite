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

package org.apache.ignite.internal.processors.cache;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateProposedMessage;
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

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** */
public class BinaryMetadataDelayedUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final int GRIDS = 3;

    /** */
    private static final int FIELDS = 2;

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private static final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setIncludeEventTypes(EventType.EVTS_DISCOVERY);

        BlockTcpDiscoverySpi spi = new BlockTcpDiscoverySpi();
        spi.skipAddressesRandomization = true;

        cfg.setDiscoverySpi(spi.setIpFinder(ipFinder));

        cfg.setClientMode(igniteInstanceName.startsWith("client"));

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

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(50 * MB)));

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setBackups(0).
            setQueryEntities(Collections.singleton(qryEntity)).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
            setCacheMode(CacheMode.PARTITIONED));

        return cfg;
    }

    /** Flag to start syncing metadata requests. Should skip on exchange. */
    private volatile boolean syncMeta;

    /** Metadata init latch. Both threads must request initial metadata. */
    private CountDownLatch initMetaReq = new CountDownLatch(2);

    /** Thread local flag for need of waiting local metadata update. */
    private ThreadLocal<Boolean> delayMetadataUpdateThreadLoc = new ThreadLocal<>();

    /** Latch for waiting local metadata update. */
    public static final CountDownLatch localMetaUpdatedLatch = new CountDownLatch(1);

    /** Latch for waiting for tx finish (error or success). */
    public static final CountDownLatch txFinishLatch = new CountDownLatch(1);

    /** */
    public void testMissingSchemaUpdate() throws Exception {
        // Start order is important.
        Ignite node0 = startGrid("node0");

        Ignite node1 = startGrid("node1");

        IgniteEx client0 = startGrid("client0");

        CacheObjectBinaryProcessorImpl.TestBinaryContext clientCtx =
            (CacheObjectBinaryProcessorImpl.TestBinaryContext)((CacheObjectBinaryProcessorImpl)client0.context().
                cacheObjects()).binaryContext();

        clientCtx.addListener(new CacheObjectBinaryProcessorImpl.TestBinaryContext.TestBinaryContextListener() {
            @Override public void onAfterMetadataRequest(int typeId, BinaryType type) {
                if (syncMeta) {
                    try {
                        initMetaReq.countDown();

                        initMetaReq.await();
                    }
                    catch (Exception e) {
                        throw new BinaryObjectException(e);
                    }
                }
            }

            @Override public void onBeforeMetadataUpdate(int typeId, BinaryMetadata metadata) {
                // Delay one of updates until schema is locally updated on propose message.
                if (delayMetadataUpdateThreadLoc.get() != null)
                    U.awaitQuiet(localMetaUpdatedLatch);
            }
        });

        Ignite node2 = startGrid("node2");

        Ignite node3 = startGrid("node3");

        Ignite node4 = startGrid("node4");

        node0.cluster().active(true);

        awaitPartitionMapExchange();

        syncMeta = true;

        CountDownLatch clientProposeMsgBlockedLatch = new CountDownLatch(1);

        AtomicBoolean clientWait = new AtomicBoolean();
        final Object clientMux = new Object();

        AtomicBoolean srvWait = new AtomicBoolean();
        final Object srvMux = new Object();

        ((BlockTcpDiscoverySpi)node1.configuration().getDiscoverySpi()).setBlockPredicate((snd, msg) -> {
            if (msg instanceof MetadataUpdateProposedMessage) {
                if (Thread.currentThread().getName().contains("client")) {
                    log.info("Block custom message to client0: [locNode=" + snd + ", msg=" + msg + ']');

                    clientProposeMsgBlockedLatch.countDown();

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

                return true;
            }

            return false;
        });

        ((BlockTcpDiscoverySpi)node2.configuration().getDiscoverySpi()).setBlockPredicate((snd, msg) -> {
            if (msg instanceof MetadataUpdateProposedMessage) {
                MetadataUpdateProposedMessage msg0 = (MetadataUpdateProposedMessage)msg;

                int pendingVer = U.field(msg0, "pendingVer");

                // Should not block propose messages until they reach coordinator.
                if (pendingVer == 0)
                    return false;

                log.info("Block custom message to next server: [locNode=" + snd + ", msg=" + msg + ']');

                // Message to client
                synchronized (srvMux) {
                    while (!srvWait.get())
                        try {
                            srvMux.wait();
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                }

                return true;
            }

            return false;
        });

        Integer key = primaryKey(node3.cache(DEFAULT_CACHE_NAME));

        IgniteInternalFuture fut0 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = client0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    client0.cache(DEFAULT_CACHE_NAME).put(key, build(client0, "val", 0));

                    tx.commit();
                }
                catch (Throwable t) {
                    log.error("err", t);
                }

            }
        });

        // Implements test logic.
        IgniteInternalFuture fut1 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                // Wait for initial metadata received. It should be initial version: pending=0, accepted=0
                U.awaitQuiet(initMetaReq);

                // Wait for blocking proposal message to client node.
                U.awaitQuiet(clientProposeMsgBlockedLatch);

                // Ublock proposal message to client.
                clientWait.set(true);

                synchronized (clientMux) {
                    clientMux.notify();
                }

                // Give some time to apply update.
                doSleep(3000);

                // Unblock second transaction for metadata update.
                localMetaUpdatedLatch.countDown();

                // This tx will start committing and should fail because metadata is not present on node3.
                U.awaitQuiet(txFinishLatch);

                //
                srvWait.set(true);
                synchronized (srvMux) {
                    srvMux.notify();
                }
            }
        });

        IgniteInternalFuture fut2 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                delayMetadataUpdateThreadLoc.set(true);

                try (Transaction tx = client0.transactions().
                    txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                    client0.cache(DEFAULT_CACHE_NAME).put(key, build(client0, "val", 0));

                    tx.commit();
                }
                finally {
                    txFinishLatch.countDown();
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

        System.setProperty(IgniteSystemProperties.IGNITE_TEST_FEATURES_ENABLED, "true");
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(IgniteSystemProperties.IGNITE_TEST_FEATURES_ENABLED);

        stopAllGrids();
    }

    public BinaryContext binaryContext(IgniteEx ex) {
        return ((CacheObjectBinaryProcessorImpl)ex.context().cacheObjects()).binaryContext();
    }
}
