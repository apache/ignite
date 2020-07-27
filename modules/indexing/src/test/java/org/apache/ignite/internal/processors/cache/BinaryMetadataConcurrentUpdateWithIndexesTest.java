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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateProposedMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.BlockTcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests scenario for too early metadata update completion in case of multiple concurrent updates for the same schema.
 * <p>
 * Scenario is the following:
 *
 * <ul>
 *     <li>Start 4 nodes, connect client to node 2 in topology order (starting from 1).</li>
 *     <li>Start two concurrent transactions from client node producing same schema update.</li>
 *     <li>Delay second update until first update will return to client with stamped propose message and writes new
 *     schema to local metadata cache</li>
 *     <li>Unblock second update. It should correctly wait until the metadata is applied on all
 *     nodes or tx will fail on commit.</li>
 * </ul>
 */
public class BinaryMetadataConcurrentUpdateWithIndexesTest extends GridCommonAbstractTest {
    /** */
    private static final int FIELDS = 2;

    /** */
    private static final int MB = 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setIncludeEventTypes(EventType.EVTS_DISCOVERY);

        BlockTcpDiscoverySpi spi = new BlockTcpDiscoverySpi();

        Field rndAddrsField = U.findField(BlockTcpDiscoverySpi.class, "skipAddrsRandomization");

        assertNotNull(rndAddrsField);

        rndAddrsField.set(spi, true);

        cfg.setDiscoverySpi(spi.setIpFinder(sharedStaticIpFinder));

        QueryEntity qryEntity = new QueryEntity("java.lang.Integer", "Value");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        Collection<QueryIndex> indexes = new ArrayList<>(FIELDS);

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

    /** */
    @Test
    public void testMissingSchemaUpdate() throws Exception {
        // Start order is important.
        Ignite node0 = startGrid("node0");

        Ignite node1 = startGrid("node1");

        IgniteEx client0 = startClientGrid("client0");

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
                    await(localMetaUpdatedLatch, 5000);
            }
        });

        Ignite node2 = startGrid("node2");

        Ignite node3 = startGrid("node3");

        startGrid("node4");

        node0.cluster().active(true);

        awaitPartitionMapExchange();

        syncMeta = true;

        CountDownLatch clientProposeMsgBlockedLatch = new CountDownLatch(1);

        AtomicBoolean clientWait = new AtomicBoolean();
        final Object clientMux = new Object();

        AtomicBoolean srvWait = new AtomicBoolean();
        final Object srvMux = new Object();

        ((BlockTcpDiscoverySpi)node1.configuration().getDiscoverySpi()).setClosure((snd, msg) -> {
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
                                fail();
                            }
                    }
                }
            }

            return null;
        });

        ((BlockTcpDiscoverySpi)node2.configuration().getDiscoverySpi()).setClosure((snd, msg) -> {
            if (msg instanceof MetadataUpdateProposedMessage) {
                MetadataUpdateProposedMessage msg0 = (MetadataUpdateProposedMessage)msg;

                int pendingVer = U.field(msg0, "pendingVer");

                // Should not block propose messages until they reach coordinator.
                if (pendingVer == 0)
                    return null;

                log.info("Block custom message to next server: [locNode=" + snd + ", msg=" + msg + ']');

                // Message to client
                synchronized (srvMux) {
                    while (!srvWait.get())
                        try {
                            srvMux.wait();
                        }
                        catch (InterruptedException e) {
                            fail();
                        }
                }
            }

            return null;
        });

        Integer key = primaryKey(node3.cache(DEFAULT_CACHE_NAME));

        IgniteInternalFuture fut0 = runAsync(() -> {
            try (Transaction tx = client0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                client0.cache(DEFAULT_CACHE_NAME).put(key, build(client0, "val", 0));

                tx.commit();
            }
            catch (Throwable t) {
                log.error("err", t);
            }

        });

        // Implements test logic.
        IgniteInternalFuture fut1 = runAsync(() -> {
            // Wait for initial metadata received. It should be initial version: pending=0, accepted=0
            await(initMetaReq, 5000);

            // Wait for blocking proposal message to client node.
            await(clientProposeMsgBlockedLatch, 5000);

            // Unblock proposal message to client.
            clientWait.set(true);

            synchronized (clientMux) {
                clientMux.notify();
            }

            // Give some time to apply update.
            doSleep(3000);

            // Unblock second metadata update.
            localMetaUpdatedLatch.countDown();

            // Give some time for tx to complete (success or fail). fut2 will throw an error if tx has failed on commit.
            doSleep(3000);

            // Unblock metadata message and allow for correct version acceptance.
            srvWait.set(true);

            synchronized (srvMux) {
                srvMux.notify();
            }
        });

        IgniteInternalFuture fut2 = runAsync(() -> {
            delayMetadataUpdateThreadLoc.set(true);

            try (Transaction tx = client0.transactions().
                txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                client0.cache(DEFAULT_CACHE_NAME).put(key, build(client0, "val", 0));

                tx.commit();
            }
        });

        fut0.get();
        fut1.get();
        fut2.get();
    }

    /**
     * @param latch Latch.
     * @param timeout Timeout.
     */
    private void await(CountDownLatch latch, long timeout) {
        try {
            latch.await(5000, MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long cnt = initMetaReq.getCount();

        if (cnt != 0)
            throw new RuntimeException("Invalid latch count after wait: " + cnt);
    }

    /**
     * @param ignite Ignite.
     * @param prefix Value prefix.
     * @param fields Fields.
     */
    protected BinaryObject build(Ignite ignite, String prefix, int... fields) {
        BinaryObjectBuilder builder = ignite.binary().builder("Value");

        for (int field : fields) {
            assertTrue(field < FIELDS);

            builder.setField("i" + field, field);
            builder.setField("s" + field, prefix + field);
        }

        return builder.build();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        CacheObjectBinaryProcessorImpl.useTestBinaryCtx = true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        CacheObjectBinaryProcessorImpl.useTestBinaryCtx = false;

        stopAllGrids();
    }
}
