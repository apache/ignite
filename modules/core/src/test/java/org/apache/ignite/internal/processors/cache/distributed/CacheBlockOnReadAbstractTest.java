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

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.ExchangeActions.CacheActionData;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public abstract class CacheBlockOnReadAbstractTest extends GridCommonAbstractTest {
    /** Default cache entries count. */
    private static final int DFLT_CACHE_ENTRIES_CNT = 2 * 1024;

    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Lazily initialized current test method. */
    private volatile Method currTestMtd;

    /** List of baseline nodes started at the beginning of the test. */
    protected final List<IgniteEx> baseline = new ArrayList<>();

    /** List of server nodes started at the beginning of the test. */
    protected final List<IgniteEx> srvs = new ArrayList<>();

    /** List of client nodes started at the beginning of the test. */
    protected final List<IgniteEx> clients = new ArrayList<>();

    /** Start node in client mode. */
    private volatile boolean startNodesInClientMode;

    /**
     * Number of baseline servers to start before test.
     *
     * @see Params#baseline()
     */
    protected int baselineServersCount() {
        return currentTestParams().baseline();
    }

    /**
     * Number of non-baseline servers to start before test.
     *
     * @see Params#servers()
     */
    protected int serversCount() {
        return currentTestParams().servers();
    }

    /**
     * Number of clients to start before test.
     *
     * @see Params#clients()
     */
    protected int clientsCount() {
        return currentTestParams().clients();
    }

    /**
     * True if cluster has to be configured with enabled persistence.
     *
     * @see Params#persistent()
     */
    protected boolean persistenceEnabled() {
        return currentTestParams().persistent();
    }

    /**
     * Number of milliseconds to warmup reading process. Used to lower fluctuations in run time. Might be 0.
     *
     * @see Params#warmup()
     */
    protected long warmup() {
        return currentTestParams().warmup();
    }

    /**
     * Number of milliseconds to wait on the potentially blocking operation.
     *
     * @see Params#timeout()
     */
    protected long timeout() {
        return currentTestParams().timeout();
    }

    /**
     * Whether the test should hang or not.
     *
     * @see Params#shouldHang()
     */
    protected boolean shouldHang() {
        return currentTestParams().shouldHang();
    }

    /**
     * @param startNodesInClientMode Start nodes on client mode.
     */
    protected void startNodesInClientMode(boolean startNodesInClientMode) {
        this.startNodesInClientMode = startNodesInClientMode;
    }

    /**
     * Annotation to configure test methods in {@link CacheBlockOnReadAbstractTest}. Its values are used throughout
     * test implementation.
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    protected @interface Params {
        /**
         * Number of baseline servers to start before test.
         */
        int baseline();

        /**
         * Number of non-baseline servers to start before test.
         */
        int servers() default 0;

        /**
         * Number of clients to start before test.
         */
        int clients() default 0;

        /**
         * True if cluster has to be configured with enabled persistence.
         */
        boolean persistent() default false;

        /**
         * Number of milliseconds to warmup reading process. Used to lower fluctuations in run time. Might be 0.
         */
        long warmup() default 500L;

        /**
         * Number of milliseconds to wait on the potentially blocking operation.
         */
        long timeout() default 3000L;

        /**
         * Whether the test should hang or not.
         */
        boolean shouldHang() default false;
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(persistenceEnabled())
                )
        );

        cfg.setClientMode(startNodesInClientMode);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();


        // Checking prerequisites.
        assertTrue("Positive timeout is required for the test.", timeout() > 0);

        assertTrue("No baseline servers were requested.", baselineServersCount() > 0);

        assertFalse(
            "All servers in non-persistent cluster are considered baseline.",
            serversCount() > 0 && !persistenceEnabled()
        );


        int idx = 0;

        // Start baseline nodes.
        for (int i = 0; i < baselineServersCount(); i++)
            baseline.add(startGrid(idx++));

        // Activate cluster.
        baseline.get(0).cluster().active(true);


        // Start server nodes in activated cluster.
        for (int i = 0; i < serversCount(); i++)
            srvs.add(startGrid(idx++));


        // Start client nodes.
        startNodesInClientMode(true);

        for (int i = 0; i < clientsCount(); i++)
            clients.add(startGrid(idx++));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        baseline.get(0).cluster().active(false);

        stopAllGrids();

        cleanPersistenceDir();
    }


    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, clients = 1)
    public void testCreateCache() throws Exception {
        doTest(createCacheBackgroundOperation(baseline.get(0)));
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 1, clients = 1, persistent = true)
    public void testCreateCachePersistent() throws Exception {
        doTest(createCacheBackgroundOperation(baseline.get(0)));
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, clients = 1, timeout = 1000L)
    public void testJoinClient() throws Exception {
        doTest(joinClientBackgroundOperation());
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 1, clients = 1, persistent = true, timeout = 1000L)
    public void testJoinClientPersistent() throws Exception {
        doTest(joinClientBackgroundOperation());
    }


    /**
     * @return Background operation that creates new cache.
     * @param ignite Ignite instance.
     */
    @NotNull private BackgroundOperation createCacheBackgroundOperation(IgniteEx ignite) {
        return new BlockMessageOnBaselineBackgroundOperation() {

            @Override protected void block() {
                ignite.createCache(UUID.randomUUID().toString());
            }

            @Override protected boolean blockMessage(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtPartitionsSingleMessage) {
                    GridDhtPartitionsSingleMessage singleMsg = (GridDhtPartitionsSingleMessage)msg;

                    GridDhtPartitionExchangeId exchangeId = singleMsg.exchangeId();

                    if (exchangeId != null) {
                        DiscoveryCustomEvent discoEvt = U.field(exchangeId, "discoEvt");

                        DiscoveryCustomMessage customMsg = discoEvt.customMessage();

                        if (customMsg instanceof DynamicCacheChangeBatch) {
                            DynamicCacheChangeBatch cacheChangeBatch = (DynamicCacheChangeBatch)customMsg;

                            ExchangeActions exchangeActions = U.field(cacheChangeBatch, "exchangeActions");

                            Collection<CacheActionData> startRequests = exchangeActions.cacheStartRequests();

                            return !startRequests.isEmpty();
                        }
                    }
                }

                return false;
            }
        };
    }

    /**
     * @return Background operation that starts new client.
     */
    @NotNull private BackgroundOperation joinClientBackgroundOperation() {
        return new BlockMessageOnBaselineBackgroundOperation() {

            @Override protected void block() {
                try {
                    startGrid(UUID.randomUUID().toString());
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }

            @Override protected boolean blockMessage(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtPartitionsFullMessage) {
                    GridDhtPartitionsFullMessage fullMsg = (GridDhtPartitionsFullMessage)msg;

                    GridDhtPartitionExchangeId exchangeId = fullMsg.exchangeId();

                    if (exchangeId != null) {
                        DiscoveryEvent discoEvt = U.field(exchangeId, "discoEvt");

                        ClusterNode evtNode = discoEvt.eventNode();

                        return evtNode != null && !evtNode.isClient();
                    }
                }

                return false;
            }
        };
    }

    /**
     * Read operation tat is going to be executed during blocking operation.
     */
    @NotNull protected abstract CacheReadBackgroundOperation getReadOperation();

    /**
     * Checks that {@code backgroundOperation} block or doesn't block read operation
     * (depending on {@link Params#shouldHang()} value). Does it for client, baseline and regular server node
     * (if persistence enabled).
     *
     * @param backgroundOperation Background operation.
     * @throws Exception If failed.
     */
    protected void doTest(BackgroundOperation backgroundOperation) throws Exception {
        CacheReadBackgroundOperation<?, ?> readOperation = getReadOperation();

        readOperation.initCache(baseline.get(0), true);

        // Warmup.
        if (warmup() > 0)
            try (AutoCloseable read = readOperation.start()) {
                Thread.sleep(warmup());
            }


        if (persistenceEnabled()) {
            doTest0(clients.get(0), readOperation, backgroundOperation);

            doTest0(srvs.get(0), readOperation, backgroundOperation);

            doTest0(baseline.get(0), readOperation, backgroundOperation);
        }
        else {
            doTest0(clients.get(0), readOperation, backgroundOperation);

            doTest0(baseline.get(0), readOperation, backgroundOperation);
        }
    }

    /**
     * Internal part for {@link CacheBlockOnReadAbstractTest#doTest(BackgroundOperation)}.
     *
     * @param ignite Ignite instance. Client / baseline / server node.
     * @param readOperation Read operation.
     * @param backgroundOperation Background operation.
     */
    private void doTest0(
        IgniteEx ignite,
        CacheReadBackgroundOperation<?, ?> readOperation,
        BackgroundOperation backgroundOperation
    ) throws Exception {
        // Reinit cache.
        readOperation.initCache(ignite, false);

        // Read while potentially blocking operation is executing.
        try (AutoCloseable block = backgroundOperation.start()) {
            try (AutoCloseable read = readOperation.start()) {
                Thread.sleep(timeout());
            }
        }

        System.out.println("|> fin " + readOperation.readOperationsFinished());
        System.out.println("|> max " + readOperation.maxReadDuration() + "ms");

        // None of read operations should fail.
        assertEquals(0, readOperation.readOperationsFailed());

        // There has to be at least one successfully finished read operation.
        assertTrue(readOperation.readOperationsFinished() > 0);

        if (shouldHang())
            // One of read operations lasted about as long as blocking timeout.
            assertTrue(readOperation.maxReadDuration() > timeout() * 0.9);
        else {
            // There were no operations as long as blocking timeout.
            assertTrue(readOperation.maxReadDuration() < timeout() * 0.75);

            // On average every read operation was much faster the blocking timeout.
            double avgDuration = (double)timeout() / readOperation.readOperationsFinished();

            assertTrue(avgDuration < timeout() * 0.1);
        }
    }

    /**
     * Utility class that allows to start and stop some background operation many times.
     */
    protected abstract static class BackgroundOperation {
        /** */
        private IgniteInternalFuture<?> fut;

        /**
         * Invoked strictly before background thread is started.
         */
        protected void init() {
            // No-op.
        }

        /**
         * Operation itself. Will be executed in separate thread. Thread interruption has to be considered as a valid
         * way to stop operation.
         */
        protected abstract void execute();

        /**
         * @return Allowed time to wait in {@link BackgroundOperation#stop()} method before canceling background thread.
         */
        protected abstract long stopTimeout();

        /**
         * Start separate thread and execute method {@link BackgroundOperation#execute()} in it.
         *
         * @return {@link AutoCloseable} that invokes {@link BackgroundOperation#stop()} on closing.
         */
        AutoCloseable start() {
            if (fut != null)
                throw new UnsupportedOperationException("Only one simultanious operation is allowed");

            init();

            CountDownLatch threadStarted = new CountDownLatch(1);

            fut = GridTestUtils.runAsync(() -> {
                try {
                    threadStarted.countDown();

                    execute();
                }
                catch (Exception e) {
                    throw new IgniteException("Unexpected exception in background operation thread", e);
                }
            });

            try {
                threadStarted.await();
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }

            return this::stop;
        }

        /**
         * Interrupt the operation started in {@link BackgroundOperation#start()} method and join interrupted thread.
         */
        void stop() throws Exception {
            if (fut == null)
                return;

            try {
                fut.get(stopTimeout());
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                fut.cancel();

                fut.get();
            }
            finally {
                fut = null;
            }
        }
    }

    /**
     * Background operation that executes some node request and doesn't allow its messages to be fully processed until
     * operation is stopped.
     */
    protected abstract class BlockMessageOnBaselineBackgroundOperation extends BackgroundOperation {

        /** {@inheritDoc} */
        @Override protected void execute() {
            for (IgniteEx server : baseline) {
                TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(server);

                spi.blockMessages(this::blockMessage);
            }

            block();
        }

        /**
         * Function to pass into {@link TestRecordingCommunicationSpi#blockMessages(IgniteBiPredicate)}.
         *
         * @param node Node that receives message.
         * @param msg Message.
         * @return Whether the given message should be blocked or not.
         */
        protected abstract boolean blockMessage(ClusterNode node, Message msg);

        /**
         * Operation which messages have to be blocked.
         */
        protected abstract void block();

        /** {@inheritDoc} */
        @Override protected long stopTimeout() {
            return 1000L;
        }

        /** {@inheritDoc} */
        @Override void stop() throws Exception {
            for (IgniteEx server : baseline) {
                TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(server);

                spi.stopBlock();
            }

            super.stop();
        }
    }



    /**
     * {@link BackgroundOperation} implementation for cache reading operations.
     */
    protected abstract static class ReadBackgroundOperation extends BackgroundOperation {

        /** Counter for successfully finished operations. */
        private final AtomicInteger readOperationsFinished = new AtomicInteger();

        /** Counter for failed operations. */
        private final AtomicInteger readOperationsFailed = new AtomicInteger();

        /** Duration of the longest read operation. */
        private final AtomicLong maxReadDuration = new AtomicLong(-1);

        /**
         * Do single iteration of reading operation. Will be executed in a loop.
         */
        protected abstract void doRead() throws Exception;


        /** {@inheritDoc} */
        @Override protected void init() {
            readOperationsFinished.set(0);

            readOperationsFailed.set(0);

            maxReadDuration.set(-1);
        }

        /** {@inheritDoc} */
        @Override protected void execute() {
            long prevTs = System.currentTimeMillis();

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    doRead();

                    readOperationsFinished.incrementAndGet();
                }
                catch (Exception e) {
                    if (X.hasCause(e, InterruptedException.class, IgniteInterruptedException.class))
                        Thread.currentThread().interrupt();
                    else {
                        readOperationsFailed.incrementAndGet();

                        e.printStackTrace();
                    }
                }

                long currTs = System.currentTimeMillis();

                maxReadDuration.set(Math.max(maxReadDuration.get(), currTs - prevTs));

                prevTs = currTs;
            }
        }

        /** {@inheritDoc} */
        @Override protected long stopTimeout() {
            return 0;
        }

        /**
         * @return Number of successfully finished operations.
         */
        public int readOperationsFinished() {
            return readOperationsFinished.get();
        }

        /**
         * @return Number of failed operations.
         */
        public int readOperationsFailed() {
            return readOperationsFailed.get();
        }

        /**
         * @return Duration of the longest read operation.
         */
        public long maxReadDuration() {
            return maxReadDuration.get();
        }
    }

    /**
     *
     */
    protected abstract static class CacheReadBackgroundOperation<KeyType, ValueType> extends ReadBackgroundOperation {
        /**
         * {@link CacheReadBackgroundOperation#cache()} method backing field. Updated on each
         * {@link CacheReadBackgroundOperation#initCache(IgniteEx, boolean)} invocation.
         */
        private IgniteCache<KeyType, ValueType> cache;


        /**
         * Reinit internal cache using passed ignite node and fill it with data if required.
         *
         * @param ignite Node to get or create cache from.
         * @param fillData Whether the cache should be filled with new data or not.
         */
        public void initCache(IgniteEx ignite, boolean fillData) {
            cache = ignite.getOrCreateCache(createCacheConfiguration());

            if (fillData) {
                try (IgniteDataStreamer<KeyType, ValueType> dataStreamer = ignite.dataStreamer(cache.getName())) {
                    dataStreamer.allowOverwrite(true);

                    for (int i = 0; i < entriesCount(); i++)
                        dataStreamer.addData(createKey(i), createValue(i));
                }
            }
        }

        /**
         * @return Cache configuration.
         */
        protected CacheConfiguration<KeyType, ValueType> createCacheConfiguration() {
            return new CacheConfiguration<KeyType, ValueType>(DEFAULT_CACHE_NAME)
                .setBackups(1);
        }

        /**
         * @return Current cache.
         */
        protected final IgniteCache<KeyType, ValueType> cache() {
            return cache;
        }

        /**
         * @return Count of cache entries to create in
         *      {@link CacheReadBackgroundOperation#initCache(IgniteEx, boolean)} method.
         */
        protected int entriesCount() {
            return DFLT_CACHE_ENTRIES_CNT;
        }

        /**
         * @param idx Unique number.
         * @return Key to be used for inserting into cache.
         * @see CacheReadBackgroundOperation#createValue(int)
         */
        protected abstract KeyType createKey(int idx);

        /**
         * @param idx Unique number.
         * @return Value to be used for inserting into cache.
         * @see CacheReadBackgroundOperation#createKey(int)
         */
        protected abstract ValueType createValue(int idx);
    }

    /**
     * {@link CacheReadBackgroundOperation} implementation for (int -> int) cache. Keys and values are equal by default.
     */
    protected abstract static class IntCacheReadBackgroundOperation
        extends CacheReadBackgroundOperation<Integer, Integer> {
        /** {@inheritDoc} */
        @Override protected Integer createKey(int idx) {
            return idx;
        }

        /** {@inheritDoc} */
        @Override protected Integer createValue(int idx) {
            return idx;
        }
    }


    /**
     * @return Current test method.
     * @throws NoSuchMethodError If method wasn't found for some reason.
     */
    @NotNull protected Method currentTestMethod() {
        if (currTestMtd == null)
            try {
                currTestMtd = getClass().getMethod(getName());
            }
            catch (NoSuchMethodException e) {
                throw new NoSuchMethodError("Current test method is not found: " + getName());
            }
        return currTestMtd;
    }

    /**
     * Search for the annotation of the given type in current test method.
     *
     * @param annotationCls Type of annotation to look for.
     * @param <A> Annotation type.
     * @return Instance of annotation if it is present in test method.
     */
    protected <A extends Annotation> A currentTestAnnotation(Class<A> annotationCls) {
        return currentTestMethod().getAnnotation(annotationCls);
    }

    /**
     * @return {@link Params} annotation object from the current test method.
     */
    protected Params currentTestParams() {
        Params params = currentTestAnnotation(Params.class);

        assertNotNull("Test " + getName() + " is not annotated with @Param annotation.", params);

        return params;
    }
}
