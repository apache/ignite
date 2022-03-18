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

package org.apache.ignite.internal.processors.metastorage;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.processors.metastorage.persistence.DmsDataWriterWorker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Test for {@link DistributedMetaStorageImpl} with disabled persistence.
 */
public class DistributedMetaStorageTest extends GridCommonAbstractTest {
    /**
     * Used in tests for updatesCount counter of metastorage and corresponds to keys BASELINE_ENABLED and other initial
     * objects that were added but should not be counted along with keys defined in tests.
     */
    private static int initialUpdatesCount = -1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteEx ign = startGrid(0);

        // We have to start the second node and wait when it is started
        // to be sure that all async metastorage updates of the node_0 are completed.
        startGrid(1);

        //baselineAutoAdjustEnabled
        //baselineAutoAdjustTimeout
        //historical.rebalance.threshold
        waitForCondition(() -> (int)metastorage(0).getUpdatesCount() == 3, 10_000);

        initialUpdatesCount = (int)metastorage(0).getUpdatesCount();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(isPersistent())
            )
            .setWalSegments(3)
            .setWalSegmentSize(512 * 1024)
        );

        DiscoverySpi discoSpi = cfg.getDiscoverySpi();

        if (discoSpi instanceof TcpDiscoverySpi)
            ((TcpDiscoverySpi)discoSpi).setNetworkTimeout(1000);

        return cfg;
    }

    /**
     * @return {@code true} for tests with persistent cluster, {@code false} otherwise.
     */
    protected boolean isPersistent() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /** */
    @Before
    public void before() throws Exception {
        stopAllGrids();
    }

    /** */
    @After
    public void after() throws Exception {
        stopAllGrids(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNode() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        DistributedMetaStorage metastorage = ignite.context().distributedMetastorage();

        assertNull(metastorage.read("key"));

        metastorage.write("key", "value");

        assertEquals("value", metastorage.read("key"));

        metastorage.remove("key");

        assertNull(metastorage.read("key"));

        stopGrid(0);

        try {
            metastorage.writeAsync("key", "value").get(10, TimeUnit.SECONDS);

            fail("Exception is expected");
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, NodeStoppingException.class));

            assertTrue(e.getMessage().contains("Node is stopping."));
        }
    }

    /**
     * Test verifies that Distributed Metastorage on client yields error if client is not connected to some cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDistributedMetastorageOperationsOnClient() throws Exception {
        String clientName = "client0";

        String key = "key";
        String value = "value";

        GridTestUtils.runAsync(() -> startClientGrid(clientName));

        boolean dmsStarted = waitForCondition(() -> {
            try {
                IgniteKernal clientGrid = IgnitionEx.gridx(clientName);

                return clientGrid != null
                    && clientGrid.context().distributedMetastorage() != null
                    && clientGrid.context().discovery().localNode() != null;
            }
            catch (Exception ignored) {
                return false;
            }
        }, 20_000);

        assertTrue(dmsStarted);

        IgniteKernal cl0 = IgnitionEx.gridx("client0");

        final DistributedMetaStorage clDms = cl0.context().distributedMetastorage();

        assertNotNull(clDms);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                clDms.write(key, value);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodes() throws Exception {
        int cnt = 4;

        startGrids(cnt);

        grid(0).cluster().active(true);

        for (int i = 0; i < cnt; i++) {
            String key = UUID.randomUUID().toString();

            String val = UUID.randomUUID().toString();

            metastorage(i).write(key, val);

            for (int j = 0; j < cnt; j++)
                assertEquals(i + " " + j, val, metastorage(j).read(key));
        }

        for (int i = 1; i < cnt; i++)
            assertDistributedMetastoragesAreEqual(grid(0), grid(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testListenersOnWrite() throws Exception {
        int cnt = 4;

        startGrids(cnt);

        grid(0).cluster().active(true);

        AtomicInteger predCntr = new AtomicInteger();

        for (int i = 0; i < cnt; i++) {
            DistributedMetaStorage metastorage = metastorage(i);

            metastorage.listen(key -> key.startsWith("k"), (key, oldVal, newVal) -> {
                assertNull(oldVal);

                assertEquals("value", newVal);

                predCntr.incrementAndGet();
            });
        }

        metastorage(0).write("key", "value");

        assertEquals(cnt, predCntr.get());

        for (int i = 1; i < cnt; i++)
            assertDistributedMetastoragesAreEqual(grid(0), grid(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testListenersOnRemove() throws Exception {
        int cnt = 4;

        startGrids(cnt);

        grid(0).cluster().active(true);

        metastorage(0).write("key", "value");

        AtomicInteger predCntr = new AtomicInteger();

        for (int i = 0; i < cnt; i++) {
            DistributedMetaStorage metastorage = metastorage(i);

            metastorage.listen(key -> key.startsWith("k"), (key, oldVal, newVal) -> {
                assertEquals("value", oldVal);

                assertNull(newVal);

                predCntr.incrementAndGet();
            });
        }

        metastorage(0).remove("key");

        assertEquals(cnt, predCntr.get());

        for (int i = 1; i < cnt; i++)
            assertDistributedMetastoragesAreEqual(grid(0), grid(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCas() throws Exception {
        startGrids(2);

        grid(0).cluster().active(true);

        assertFalse(metastorage(0).compareAndSet("key", "expVal", "newVal"));

        assertNull(metastorage(0).read("key"));

        assertFalse(metastorage(0).compareAndRemove("key", "expVal"));

        assertTrue(metastorage(0).compareAndSet("key", null, "val1"));

        assertEquals("val1", metastorage(0).read("key"));

        assertFalse(metastorage(0).compareAndSet("key", null, "val2"));

        assertEquals("val1", metastorage(0).read("key"));

        assertTrue(metastorage(0).compareAndSet("key", "val1", "val3"));

        assertEquals("val3", metastorage(0).read("key"));

        assertFalse(metastorage(0).compareAndRemove("key", "val1"));

        assertEquals("val3", metastorage(0).read("key"));

        assertTrue(metastorage(0).compareAndRemove("key", "val3"));

        assertNull(metastorage(0).read("key"));

        assertDistributedMetastoragesAreEqual(grid(0), grid(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinCleanNode() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.context().distributedMetastorage().write("key", "value");

        IgniteEx newNode = startGrid(1);

        assertEquals("value", newNode.context().distributedMetastorage().read("key"));

        assertDistributedMetastoragesAreEqual(ignite, newNode);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, value = "0")
    public void testJoinCleanNodeFullData() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.context().distributedMetastorage().write("key1", "value1");

        ignite.context().distributedMetastorage().write("key2", "value2");

        startGrid(1);

        assertEquals("value1", metastorage(1).read("key1"));

        assertEquals("value2", metastorage(1).read("key2"));

        assertDistributedMetastoragesAreEqual(ignite, grid(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, value = "0")
    public void testDeactivateActivate() throws Exception {
        startGrid(0);

        grid(0).cluster().active(true);

        metastorage(0).write("key1", "value1");

        metastorage(0).write("key2", "value2");

        grid(0).cluster().active(false);

        startGrid(1);

        grid(0).cluster().active(true);

        assertEquals("value1", metastorage(0).read("key1"));

        assertEquals("value2", metastorage(0).read("key2"));

        assertDistributedMetastoragesAreEqual(grid(0), grid(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimizedWriteTwice() throws Exception {
        IgniteEx igniteEx = startGrid(0);

        igniteEx.cluster().state(ClusterState.ACTIVE);

        metastorage(0).write("key1", "value1");

        int expUpdatesCnt = isPersistent() ? 2 : 1;

        assertTrue("initialUpdatesCount=" + initialUpdatesCount + ", upd=" + metastorage(0).getUpdatesCount(),
            waitForCondition(() ->
            (expUpdatesCnt == metastorage(0).getUpdatesCount() - initialUpdatesCount), 10_000));

        metastorage(0).write("key2", "value2");

        assertEquals(expUpdatesCnt + 1, metastorage(0).getUpdatesCount() - initialUpdatesCount);

        metastorage(0).write("key1", "value1");

        assertEquals(expUpdatesCnt + 1, metastorage(0).getUpdatesCount() - initialUpdatesCount);
    }

    /** */
    @Test
    public void testClient() throws Exception {
        IgniteEx igniteEx = startGrid(0);

        igniteEx.cluster().state(ClusterState.ACTIVE);

        checkStored(metastorage(0), metastorage(0), "key0", "value0");

        startClientGrid(1);

        AtomicInteger clientLsnrUpdatesCnt = new AtomicInteger();

        int expUpdatesCnt = isPersistent() ? 2 : 1;

        assertEquals("initialUpdatesCount=" + initialUpdatesCount + ", upd=" + metastorage(1).getUpdatesCount(),
            expUpdatesCnt, metastorage(1).getUpdatesCount() - initialUpdatesCount);

        assertEquals("value0", metastorage(1).read("key0"));

        metastorage(1).listen(key -> true, (key, oldVal, newVal) -> clientLsnrUpdatesCnt.incrementAndGet());

        checkStored(metastorage(1), metastorage(1), "key1", "value1");

        checkStored(metastorage(1), metastorage(0), "key1", "value1");

        assertEquals(1, clientLsnrUpdatesCnt.get());
    }

    /** */
    protected void checkStoredWithPers(
        DistributedMetaStorage msToStore,
        IgniteEx instanceToCheck,
        String key,
        String value
    ) throws IgniteCheckedException {
        assertTrue(isPersistent());

        final CountDownLatch latch = new CountDownLatch(1);

        final DistributedMetaStorageImpl distrMetaStore =
            (DistributedMetaStorageImpl)instanceToCheck.context().distributedMetastorage();

        DmsDataWriterWorker worker = GridTestUtils.getFieldValue(distrMetaStore, "worker");

        ReadWriteMetastorage metastorage = GridTestUtils.getFieldValue(worker, "metastorage");

        assertNotNull(metastorage);

        IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
            try {
                latch.await();

                assertTrue(waitForCondition(() -> {
                    try {
                        AtomicReference<String> contains = new AtomicReference<>();

                        metastorage.iterate("", (k, v) -> {
                            if (k.contains(key))
                                contains.set(k);
                        }, false);

                        return contains.get() != null && metastorage.readRaw(contains.get()) != null;
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }, 15_000));
            }
            catch (IgniteInterruptedCheckedException | InterruptedException e) {
                throw new IgniteException(e);
            }
        });

        latch.countDown();

        msToStore.write(key, value);

        f.get();
    }

    /** */
    protected void checkStored(
        DistributedMetaStorage msToStore,
        DistributedMetaStorage msToCheck,
        String key,
        String value
    ) throws IgniteCheckedException {
        final CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
            try {
                latch.await();

                assertTrue(waitForCondition(() -> {
                    try {
                        AtomicBoolean contains = new AtomicBoolean(false);
                        msToCheck.iterate("", (k, v) -> {
                            if (k.equals(key) && v.equals(value))
                                contains.set(true);
                        });

                        return contains.get();
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }, 15_000));
            }
            catch (IgniteInterruptedCheckedException | InterruptedException e) {
                throw new IgniteException(e);
            }
        });

        latch.countDown();

        msToStore.write(key, value);

        f.get();
    }

    /** */
    @Test
    public void testClientReconnect() throws Exception {
        IgniteEx igniteEx = startGrid(0);

        igniteEx.cluster().state(ClusterState.ACTIVE);

        if (isPersistent())
            checkStoredWithPers(metastorage(0), igniteEx, "key0", "value0");

        startGrid(2);

        stopGrid(0);

        stopGrid(2);

        IgniteEx g2 = startGrid(2);

        g2.cluster().state(ClusterState.ACTIVE);

        IgniteEx client = startClientGrid(1);

        checkStored(metastorage(2), metastorage(2), "key1", "value1");

        checkStored(metastorage(2), metastorage(2), "key2", "value2");

        assertTrue(waitForCondition(() -> client.cluster().tag() != null, 10_000));

        int expUpdatesCnt = isPersistent() ? 4 : 2;

        // Wait enough to cover failover timeout.
        assertTrue("initialUpdatesCount=" + initialUpdatesCount + ", upd=" + metastorage(1).getUpdatesCount(),
            waitForCondition(
            () -> metastorage(1).getUpdatesCount() - initialUpdatesCount == expUpdatesCnt, 10_000));

        if (isPersistent())
            assertEquals("value0", metastorage(1).read("key0"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUnstableTopology() throws Exception {
        int cnt = 8;

        startGridsMultiThreaded(cnt);

        grid(0).cluster().active(true);

        stopGrid(0);

        startGrid(0);

        AtomicInteger gridIdxCntr = new AtomicInteger(0);

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            int gridIdx = gridIdxCntr.incrementAndGet();

            try {
                while (!stop.get()) {
                    stopGrid(gridIdx, true);

                    Thread.sleep(50L);

                    startGrid(gridIdx);

                    Thread.sleep(50L);
                }
            }
            catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, cnt - 1);

        long start = System.currentTimeMillis();

        long duration = GridTestUtils.SF.applyLB(15_000, 5_000);

        try {
            while (System.currentTimeMillis() < start + duration) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                metastorage(0).write(
                    "key" + rnd.nextInt(5000), Integer.toString(rnd.nextInt(1000))
                );
            }
        }
        finally {
            stop.set(true);

            fut.get();
        }

        awaitPartitionMapExchange();

        for (int i = 1; i < cnt; i++)
            assertDistributedMetastoragesAreEqual(grid(0), grid(i));
    }

    /**
     * @return {@link DistributedMetaStorage} instance for i'th node.
     */
    protected DistributedMetaStorage metastorage(int i) {
        return grid(i).context().distributedMetastorage();
    }

    /**
     * Assert that two nodes have the same internal state in {@link DistributedMetaStorage}.
     */
    protected void assertDistributedMetastoragesAreEqual(IgniteEx ignite1, IgniteEx ignite2) throws Exception {
        DistributedMetaStorage distributedMetastorage1 = ignite1.context().distributedMetastorage();

        DistributedMetaStorage distributedMetastorage2 = ignite2.context().distributedMetastorage();

        assertTrue(waitForCondition(() -> {
            Object ver1 = U.field(distributedMetastorage1, "ver");

            Object ver2 = U.field(distributedMetastorage2, "ver");

            return ver1.equals(ver2);
        }, 10_000));

        assertTrue(waitForCondition(() -> {
            Object histCache1 = U.field(distributedMetastorage1, "histCache");

            Object histCache2 = U.field(distributedMetastorage2, "histCache");

            return histCache1.equals(histCache2);
        }, 10_000));

        assertTrue(waitForCondition(() -> {
            Method fullDataMtd = U.findNonPublicMethod(DistributedMetaStorageImpl.class, "localFullData");

            try {
                Object[] fullData1 = (Object[])fullDataMtd.invoke(distributedMetastorage1);

                Object[] fullData2 = (Object[])fullDataMtd.invoke(distributedMetastorage2);

                boolean pr1 = F.eqOrdered(Arrays.asList(fullData1), Arrays.asList(fullData2));

                // Also check that arrays are sorted.
                Arrays.sort(fullData1, Comparator.comparing(o -> U.field(o, "key")));

                boolean pr2 = F.eqOrdered(Arrays.asList(fullData1), Arrays.asList(fullData2));

                return pr1 && pr2;
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new IgniteException(e);
            }
        }, 10_000));
    }
}
