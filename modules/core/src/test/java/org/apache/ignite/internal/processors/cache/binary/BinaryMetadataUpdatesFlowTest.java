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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.CacheQueryEntryEvent;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class BinaryMetadataUpdatesFlowTest extends GridCommonAbstractTest {
    /** */
    private static final String SEQ_NUM_FLD = "f0";

    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private volatile boolean clientMode;

    /** */
    private volatile boolean applyDiscoveryHook;

    /** */
    private volatile DiscoveryHook discoveryHook;

    /** */
    private static final int UPDATES_COUNT = 5_000;

    /** */
    private static final int RESTART_DELAY = 500;

    /** */
    private static final int GRID_CNT = 5;

    /** */
    private static final String BINARY_TYPE_NAME = "TestBinaryType";

    /** */
    private static final int BINARY_TYPE_ID = 708045005;

    /** */
    private final Queue<BinaryUpdateDescription> updatesQueue = new ConcurrentLinkedQueue<>();

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < UPDATES_COUNT; i++) {
            FieldType fType = null;
            switch (i % 4) {
                case 0:
                    fType = FieldType.NUMBER;
                    break;
                case 1:
                    fType = FieldType.STRING;
                    break;
                case 2:
                    fType = FieldType.ARRAY;
                    break;
                case 3:
                    fType = FieldType.OBJECT;
            }

            updatesQueue.add(new BinaryUpdateDescription(i, "f" + (i + 1), fType));
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        if (applyDiscoveryHook) {
            final DiscoveryHook hook = discoveryHook != null ? discoveryHook : new DiscoveryHook();

            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
                @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
                    super.setListener(GridTestUtils.DiscoverySpiListenerWrapper.wrap(lsnr, hook));
                }
            };

            cfg.setDiscoverySpi(discoSpi);

            cfg.setMetricsUpdateFrequency(1000);
        }

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setClientMode(clientMode);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.REPLICATED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Starts computation job.
     *  @param idx Grid index on which computation job should start.
     * @param restartIdx The index of the node to be restarted.
     * @param workersCntr The current number of computation threads.
     */
    private void startComputation(int idx, AtomicInteger restartIdx, Phaser workersCntr) {
        Ignite ignite = grid(idx);

        ClusterGroup cg = ignite.cluster().forLocal();

        ignite.compute(cg).callAsync(new BinaryObjectAdder(idx, updatesQueue, 30, restartIdx, workersCntr));
    }

    /**
     * @param idx Index.
     * @param deafClient Deaf client.
     * @param observedIds Observed ids.
     * @throws Exception If failed.
     */
    private void startListening(int idx, boolean deafClient, Set<Integer> observedIds) throws Exception {
        clientMode = true;

        ContinuousQuery qry = new ContinuousQuery();

        qry.setLocalListener(new CQListener(observedIds));

        if (deafClient) {
            applyDiscoveryHook = true;
            discoveryHook = new DiscoveryHook() {
                @Override public void handleDiscoveryMessage(DiscoverySpiCustomMessage msg) {
                    DiscoveryCustomMessage customMsg = msg == null ? null
                            : (DiscoveryCustomMessage) IgniteUtils.field(msg, "delegate");

                    if (customMsg instanceof MetadataUpdateProposedMessage) {
                        if (((MetadataUpdateProposedMessage) customMsg).typeId() == BINARY_TYPE_ID)
                            GridTestUtils.setFieldValue(customMsg, "typeId", 1);
                    }
                    else if (customMsg instanceof MetadataUpdateAcceptedMessage) {
                        if (((MetadataUpdateAcceptedMessage) customMsg).typeId() == BINARY_TYPE_ID)
                            GridTestUtils.setFieldValue(customMsg, "typeId", 1);
                    }
                }
            };

            IgniteEx client = startGrid(idx);

            client.cache(DEFAULT_CACHE_NAME).withKeepBinary().query(qry);
        }
        else {
            applyDiscoveryHook = false;

            IgniteEx client = startGrid(idx);

            client.cache(DEFAULT_CACHE_NAME).withKeepBinary().query(qry);
        }
    }

    /**
     *
     */
    private static class CQListener implements CacheEntryUpdatedListener {
        /** */
        private final Set<Integer> observedIds;

        /**
         * @param observedIds
         */
        CQListener(Set<Integer> observedIds) {
            this.observedIds = observedIds;
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
            for (Object o : iterable) {
                if (o instanceof CacheQueryEntryEvent) {
                    CacheQueryEntryEvent e = (CacheQueryEntryEvent) o;

                    BinaryObjectImpl val = (BinaryObjectImpl) e.getValue();

                    Integer seqNum = val.field(SEQ_NUM_FLD);

                    observedIds.add(seqNum);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFlowNoConflicts() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        doTestFlowNoConflicts();

        awaitPartitionMapExchange();

        Ignite ignite0 = G.allGrids().get(0);

        IgniteCache<Object, Object> cache0 = ignite0.cache(DEFAULT_CACHE_NAME);

        int cacheEntries = cache0.size(CachePeekMode.PRIMARY);

        assertTrue("Cache cannot contain more entries than were put in it;", cacheEntries <= UPDATES_COUNT);

        assertEquals("There are less than expected entries, data loss occurred;", UPDATES_COUNT, cacheEntries);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFlowNoConflictsWithClients() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        if (!tcpDiscovery())
            return;

        final Set<Integer> deafClientObservedIds = new ConcurrentHashSet<>();

        startListening(5, true, deafClientObservedIds);

        final Set<Integer> regClientObservedIds = new ConcurrentHashSet<>();

        startListening(6, false, regClientObservedIds);

        doTestFlowNoConflicts();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestFlowNoConflicts() throws Exception {
        final AtomicInteger restartIdx = new AtomicInteger(-1);

        final Phaser workersCntr = new Phaser(1);

        final AtomicBoolean stopFlag = new AtomicBoolean();

        try {
            for (int i = 0; i < GRID_CNT; i++)
                startComputation(i, restartIdx, workersCntr);

            IgniteInternalFuture fut =
                GridTestUtils.runAsync(new NodeRestarter(stopFlag, restartIdx, workersCntr), "worker");

            startLatch.countDown();

            fut.get();

            workersCntr.arriveAndAwaitAdvance();
        } finally {
            stopFlag.set(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentMetadataUpdates() throws Exception {
        startGrid(0);

        final Ignite client = startGrid(getConfiguration("client").setClientMode(true));

        final IgniteCache<Integer, Object> cache = client.cache(DEFAULT_CACHE_NAME).withKeepBinary();

        int threadsNum = 10;
        final int updatesNum = 2000;

        List<IgniteInternalFuture> futs = new ArrayList<>();

        for (int i = 0; i < threadsNum; i++) {
            final int threadId = i;

            IgniteInternalFuture fut = runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        for (int j = 0; j < updatesNum; j++) {
                            BinaryObjectBuilder bob = client.binary().builder(BINARY_TYPE_NAME);

                            bob.setField("field" + j, threadId);

                            cache.put(threadId, bob.build());
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, "updater-" + i);

            futs.add(fut);
        }

        for (IgniteInternalFuture fut : futs)
            fut.get();
    }

    /**
     * Instruction for node to perform <b>add new binary object</b> action on cache in <b>keepBinary</b> mode.
     *
     * Instruction includes id the object should be added under, new field to add to binary schema
     * and {@link FieldType type} of the field.
     */
    private static final class BinaryUpdateDescription {
        /** */
        private int itemId;

        /** */
        private String fieldName;

        /** */
        private FieldType fieldType;

        /**
         * @param itemId Item id.
         * @param fieldName Field name.
         * @param fieldType Field type.
         */
        private BinaryUpdateDescription(int itemId, String fieldName, FieldType fieldType) {
            this.itemId = itemId;
            this.fieldName = fieldName;
            this.fieldType = fieldType;
        }
    }

    /**
     *
     */
    private enum FieldType {
        /** */
        NUMBER,

        /** */
        STRING,

        /** */
        ARRAY,

        /** */
        OBJECT
    }

    /**
     * Generates random number to use when creating binary object with field of numeric {@link FieldType type}.
     */
    private static int getNumberFieldVal() {
        return ThreadLocalRandom.current().nextInt(100);
    }

    /**
     * Generates random string to use when creating binary object with field of string {@link FieldType type}.
     */
    private static String getStringFieldVal() {
        return "str" + (100 + ThreadLocalRandom.current().nextInt(9));
    }

    /**
     * Generates random array to use when creating binary object with field of array {@link FieldType type}.
     */
    private static byte[] getArrayFieldVal() {
        byte[] res = new byte[3];
        ThreadLocalRandom.current().nextBytes(res);
        return res;
    }

    /**
     * @param builder Builder.
     * @param desc Descriptor with parameters of BinaryObject to build.
     * @return BinaryObject built by provided description
     */
    private static BinaryObject newBinaryObject(BinaryObjectBuilder builder, BinaryUpdateDescription desc) {
        builder.setField(SEQ_NUM_FLD, desc.itemId + 1);

        switch (desc.fieldType) {
            case NUMBER:
                builder.setField(desc.fieldName, getNumberFieldVal());
                break;
            case STRING:
                builder.setField(desc.fieldName, getStringFieldVal());
                break;
            case ARRAY:
                builder.setField(desc.fieldName, getArrayFieldVal());
                break;
            case OBJECT:
                builder.setField(desc.fieldName, new Object());
        }

        return builder.build();
    }

    /**
     * Compute job executed on each node in cluster which constantly adds new entries to ignite cache
     * according to {@link BinaryUpdateDescription descriptions} it reads from shared queue.
     */
    private final class BinaryObjectAdder implements IgniteCallable<Object> {
        /** */
        private final int idx;

        /** */
        private final Queue<BinaryUpdateDescription> updatesQueue;

        /** */
        private final long timeout;

        /** */
        private final AtomicInteger restartIdx;

        /** */
        private final Phaser workersCntr;

        /**
         * @param idx Ignite instance index.
         * @param updatesQueue Updates queue.
         * @param timeout Timeout.
         * @param restartIdx The index of the node to be restarted.
         * @param workersCntr The number of computation threads.
         */
        BinaryObjectAdder(
            int idx,
            Queue<BinaryUpdateDescription> updatesQueue,
            long timeout,
            AtomicInteger restartIdx,
            Phaser workersCntr
        ) {
            this.idx = idx;
            this.updatesQueue = updatesQueue;
            this.timeout = timeout;
            this.restartIdx = restartIdx;
            this.workersCntr = workersCntr;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            startLatch.await();

            Ignite ignite = grid(idx);

            IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME).withKeepBinary();

            workersCntr.register();

            try {
                while (!updatesQueue.isEmpty()) {
                    BinaryUpdateDescription desc = updatesQueue.poll();

                    if (desc == null)
                        break;

                    BinaryObjectBuilder builder = ignite.binary().builder(BINARY_TYPE_NAME);

                    BinaryObject bo = newBinaryObject(builder, desc);

                    cache.put(desc.itemId, bo);

                    if (restartIdx.get() == idx)
                        break;
                    else
                        Thread.sleep(timeout);
                }
            } finally {
                workersCntr.arriveAndDeregister();

                if (restartIdx.get() == idx)
                    restartIdx.set(-1);
            }

            return null;
        }
    }

    /**
     * Restarts random server node and computation job.
     */
    private class NodeRestarter implements Runnable {
        /** Stop thread flag. */
        private final AtomicBoolean stopFlag;

        /** The index of the node to be restarted. */
        private final AtomicInteger restartIdx;

        /** The current number of computation threads. */
        private final Phaser workersCntr;

        /**
         * @param stopFlag Stop thread flag.
         * @param restartIdx The index of the node to be restarted.
         * @param workersCntr The current number of computation threads.
         */
        public NodeRestarter(AtomicBoolean stopFlag, AtomicInteger restartIdx, Phaser workersCntr) {
            this.stopFlag = stopFlag;
            this.restartIdx = restartIdx;
            this.workersCntr = workersCntr;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                startLatch.await();

                while (!shouldStop()) {
                    int idx = ThreadLocalRandom.current().nextInt(5);

                    restartIdx.set(idx);

                    while (restartIdx.get() != -1) {
                        if (shouldStop())
                            return;

                        Thread.sleep(10);
                    }

                    stopGrid(idx);

                    if (shouldStop())
                        break;

                    applyDiscoveryHook = false;

                    clientMode = false;

                    startGrid(idx);

                    startComputation(idx, restartIdx, workersCntr);

                    Thread.sleep(RESTART_DELAY);
                }
            } catch (Exception ignore) {
                // No-op.
            }
        }

        /** */
        private boolean shouldStop() {
            return updatesQueue.isEmpty() || stopFlag.get() || Thread.currentThread().isInterrupted();
        }
    }
}
