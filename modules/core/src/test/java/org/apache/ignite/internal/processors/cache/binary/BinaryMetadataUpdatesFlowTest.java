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

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
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
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.IgniteUtils;
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
    private static final int RESTART_DELAY = 3_000;

    /** */
    private final Queue<BinaryUpdateDescription> updatesQueue = new LinkedBlockingDeque<>(UPDATES_COUNT);

    /** */
    private static volatile BlockingDeque<Integer> srvResurrectQueue = new LinkedBlockingDeque<>(1);

    /** */
    private static final CountDownLatch START_LATCH = new CountDownLatch(1);

    /** */
    private static final CountDownLatch FINISH_LATCH_NO_CLIENTS = new CountDownLatch(5);

    /** */
    private static volatile AtomicBoolean stopFlag0 = new AtomicBoolean(false);

    /** */
    private static volatile AtomicBoolean stopFlag1 = new AtomicBoolean(false);

    /** */
    private static volatile AtomicBoolean stopFlag2 = new AtomicBoolean(false);

    /** */
    private static volatile AtomicBoolean stopFlag3 = new AtomicBoolean(false);

    /** */
    private static volatile AtomicBoolean stopFlag4 = new AtomicBoolean(false);

    /** */
    private static final String BINARY_TYPE_NAME = "TestBinaryType";

    /** */
    private static final int BINARY_TYPE_ID = 708045005;

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
     * Starts new ignite node and submits computation job to it.
     * @param idx Index.
     * @param stopFlag Stop flag.
     */
    private void startComputation(int idx, AtomicBoolean stopFlag) throws Exception {
        clientMode = false;

        final IgniteEx ignite0 = startGrid(idx);

        ClusterGroup cg = ignite0.cluster().forNodeId(ignite0.localNode().id());

        ignite0.compute(cg).withAsync().call(new BinaryObjectAdder(ignite0, updatesQueue, 30, stopFlag));
    }

    /**
     * @param idx Index.
     * @param deafClient Deaf client.
     * @param observedIds Observed ids.
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
     *
     */
    public void testFlowNoConflicts() throws Exception {
        startComputation(0, stopFlag0);

        startComputation(1, stopFlag1);

        startComputation(2, stopFlag2);

        startComputation(3, stopFlag3);

        startComputation(4, stopFlag4);

        Thread killer = new Thread(new ServerNodeKiller());
        Thread resurrection = new Thread(new ServerNodeResurrection());
        killer.setName("node-killer-thread");
        killer.start();
        resurrection.setName("node-resurrection-thread");
        resurrection.start();

        START_LATCH.countDown();

        while (!updatesQueue.isEmpty())
            Thread.sleep(1000);

        FINISH_LATCH_NO_CLIENTS.await();

        IgniteEx ignite0 = grid(0);

        IgniteCache<Object, Object> cache0 = ignite0.cache(DEFAULT_CACHE_NAME);

        int cacheEntries = cache0.size(CachePeekMode.PRIMARY);

        assertTrue("Cache cannot contain more entries than were put in it;", cacheEntries <= UPDATES_COUNT);

        assertEquals("There are less than expected entries, data loss occurred;", UPDATES_COUNT, cacheEntries);

        killer.interrupt();
        resurrection.interrupt();
    }

    /**
     *
     */
    public void testFlowNoConflictsWithClients() throws Exception {
        startComputation(0, stopFlag0);

        startComputation(1, stopFlag1);

        startComputation(2, stopFlag2);

        startComputation(3, stopFlag3);

        startComputation(4, stopFlag4);

        final Set<Integer> deafClientObservedIds = new ConcurrentHashSet<>();

        startListening(5, true, deafClientObservedIds);

        final Set<Integer> regClientObservedIds = new ConcurrentHashSet<>();

        startListening(6, false, regClientObservedIds);

        START_LATCH.countDown();

        Thread killer = new Thread(new ServerNodeKiller());
        Thread resurrection = new Thread(new ServerNodeResurrection());
        killer.setName("node-killer-thread");
        killer.start();
        resurrection.setName("node-resurrection-thread");
        resurrection.start();

        while (!updatesQueue.isEmpty())
            Thread.sleep(1000);

        killer.interrupt();
        resurrection.interrupt();
    }

    /**
     * Runnable responsible for stopping (gracefully) server nodes during metadata updates process.
     */
    private final class ServerNodeKiller implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            Thread curr = Thread.currentThread();
            try {
                START_LATCH.await();

                while (!curr.isInterrupted()) {
                    int idx = ThreadLocalRandom.current().nextInt(5);

                    AtomicBoolean stopFlag;

                    switch (idx) {
                        case 0:
                            stopFlag = stopFlag0;
                            break;
                        case 1:
                            stopFlag = stopFlag1;
                            break;
                        case 2:
                            stopFlag = stopFlag2;
                            break;
                        case 3:
                            stopFlag = stopFlag3;
                            break;
                        default:
                            stopFlag = stopFlag4;
                    }

                    stopFlag.set(true);

                    while (stopFlag.get())
                        Thread.sleep(10);

                    stopGrid(idx);

                    srvResurrectQueue.put(idx);

                    Thread.sleep(RESTART_DELAY);
                }
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
    }

    /**
     * {@link Runnable} object to restart nodes killed by {@link ServerNodeKiller}.
     */
    private final class ServerNodeResurrection implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            Thread curr = Thread.currentThread();

            try {
                START_LATCH.await();

                while (!curr.isInterrupted()) {
                    Integer idx = srvResurrectQueue.takeFirst();

                    AtomicBoolean stopFlag;

                    switch (idx) {
                        case 0:
                            stopFlag = stopFlag0;
                            break;
                        case 1:
                            stopFlag = stopFlag1;
                            break;
                        case 2:
                            stopFlag = stopFlag2;
                            break;
                        case 3:
                            stopFlag = stopFlag3;
                            break;
                        default:
                            stopFlag = stopFlag4;
                    }

                    clientMode = false;
                    applyDiscoveryHook = false;

                    startComputation(idx, stopFlag);
                }
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
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
    private static final class BinaryObjectAdder implements IgniteCallable<Object> {
        /** */
        private final IgniteEx ignite;

        /** */
        private final Queue<BinaryUpdateDescription> updatesQueue;

        /** */
        private final long timeout;

        /** */
        private final AtomicBoolean stopFlag;

        /**
         * @param ignite Ignite.
         * @param updatesQueue Updates queue.
         * @param timeout Timeout.
         * @param stopFlag Stop flag.
         */
        BinaryObjectAdder(IgniteEx ignite, Queue<BinaryUpdateDescription> updatesQueue, long timeout, AtomicBoolean stopFlag) {
            this.ignite = ignite;
            this.updatesQueue = updatesQueue;
            this.timeout = timeout;
            this.stopFlag = stopFlag;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            START_LATCH.await();

            IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME).withKeepBinary();

            while (!updatesQueue.isEmpty()) {
                BinaryUpdateDescription desc = updatesQueue.poll();

                BinaryObjectBuilder builder = ignite.binary().builder(BINARY_TYPE_NAME);

                BinaryObject bo = newBinaryObject(builder, desc);

                cache.put(desc.itemId, bo);

                if (stopFlag.get())
                    break;
                else
                    Thread.sleep(timeout);
            }

            if (updatesQueue.isEmpty())
                FINISH_LATCH_NO_CLIENTS.countDown();

            stopFlag.set(false);

            return null;
        }
    }
}
