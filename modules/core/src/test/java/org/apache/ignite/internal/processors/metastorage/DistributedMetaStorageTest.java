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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES;

/**
 * Test for {@link DistributedMetaStorageImpl} with disabled persistence.
 */
@RunWith(JUnit4.class)
public class DistributedMetaStorageTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(isPersistent())
            )
        );

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
        stopAllGrids();

        System.clearProperty(IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES);
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
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodes() throws Exception {
        int cnt = 4;

        startGridsMultiThreaded(cnt);

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

        startGridsMultiThreaded(cnt);

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

        startGridsMultiThreaded(cnt);

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
    public void testJoinCleanNodeFullData() throws Exception {
        System.setProperty(IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, "0");

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
    public void testDeactivateActivate() throws Exception {
        System.setProperty(IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, "0");

        startGrid(0);

        grid(0).cluster().active(true);

        metastorage(0).write("key1", "value1");

        metastorage(0).write("key2", "value2");

        grid(0).cluster().active(false);

        startGrid(1);

        CountDownLatch grid1MetaStorageStartLatch = new CountDownLatch(1);

        grid(1).context().internalSubscriptionProcessor().registerDistributedMetastorageListener(
            new DistributedMetastorageLifecycleListener() {
                @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                    grid1MetaStorageStartLatch.countDown();
                }
            }
        );

        grid(0).cluster().active(true);

        assertEquals("value1", metastorage(0).read("key1"));

        assertEquals("value2", metastorage(0).read("key2"));

        grid1MetaStorageStartLatch.await(1, TimeUnit.SECONDS);

        assertDistributedMetastoragesAreEqual(grid(0), grid(1));
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

        Object ver1 = U.field(distributedMetastorage1, "ver");

        Object ver2 = U.field(distributedMetastorage2, "ver");

        assertEquals(ver1, ver2);

        Object histCache1 = U.field(distributedMetastorage1, "histCache");

        Object histCache2 = U.field(distributedMetastorage2, "histCache");

        assertEquals(histCache1, histCache2);

        Method fullDataMtd = U.findNonPublicMethod(DistributedMetaStorageImpl.class, "localFullData");

        Object[] fullData1 = (Object[])fullDataMtd.invoke(distributedMetastorage1);

        Object[] fullData2 = (Object[])fullDataMtd.invoke(distributedMetastorage2);

        assertEqualsCollections(Arrays.asList(fullData1), Arrays.asList(fullData2));

        // Also check that arrays are sorted.
        Arrays.sort(fullData1, Comparator.comparing(o -> U.field(o, "key")));

        assertEqualsCollections(Arrays.asList(fullData1), Arrays.asList(fullData2));
    }
}
