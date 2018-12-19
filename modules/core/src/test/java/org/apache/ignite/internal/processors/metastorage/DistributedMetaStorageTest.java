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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES;

/** */
@RunWith(JUnit4.class)
public class DistributedMetaStorageTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(isPersistent())
            )
        );

        return cfg;
    }

    /** */
    protected boolean isPersistent() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testSingleNode() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        DistributedMetaStorage metastorage = ignite.context().globalMetastorage();

        assertNull(metastorage.read("key"));

        metastorage.write("key", "value");

        assertEquals("value", metastorage.read("key"));

        metastorage.remove("key");

        assertNull(metastorage.read("key"));
    }

    /** */
    @Test
    public void testMultipleNodes() throws Exception {
        int cnt = 4;

        startGridsMultiThreaded(cnt);

        grid(0).cluster().active(true);

        for (int i = 0; i < cnt; i++) {
            String key = UUID.randomUUID().toString();

            String val = UUID.randomUUID().toString();

            metastorage(i).write(key, val);

            Thread.sleep(150L); // Remove later.

            for (int j = 0; j < cnt; j++)
                assertEquals(i + " " + j, val, metastorage(j).read(key));
        }

        for (int i = 1; i < cnt; i++)
            assertGlobalMetastoragesAreEqual(grid(0), grid(i));
    }

    /** */
    @Test
    public void testListenersOnWrite() throws Exception {
        int cnt = 4;

        startGridsMultiThreaded(cnt);

        grid(0).cluster().active(true);

        AtomicInteger predCntr = new AtomicInteger();

        for (int i = 0; i < cnt; i++) {
            DistributedMetaStorage metastorage = grid(i).context().globalMetastorage();

            metastorage.listen(key -> key.startsWith("k"), (key, val) -> {
                assertEquals("value", val);

                predCntr.incrementAndGet();
            });
        }

        grid(0).context().globalMetastorage().write("key", "value");

        Thread.sleep(150L); // Remove later.

        assertEquals(cnt, predCntr.get());

        for (int i = 1; i < cnt; i++)
            assertGlobalMetastoragesAreEqual(grid(0), grid(i));
    }

    /** */
    @Test
    public void testListenersOnRemove() throws Exception {
        int cnt = 4;

        startGridsMultiThreaded(cnt);

        grid(0).cluster().active(true);

        grid(0).context().globalMetastorage().write("key", "value");

        Thread.sleep(150L); // Remove later.

        AtomicInteger predCntr = new AtomicInteger();

        for (int i = 0; i < cnt; i++) {
            DistributedMetaStorage metastorage = grid(i).context().globalMetastorage();

            metastorage.listen(key -> key.startsWith("k"), (key, val) -> {
                assertNull(val);

                predCntr.incrementAndGet();
            });
        }

        grid(0).context().globalMetastorage().remove("key");

        Thread.sleep(150L); // Remove later.

        assertEquals(cnt, predCntr.get());

        for (int i = 1; i < cnt; i++)
            assertGlobalMetastoragesAreEqual(grid(0), grid(i));
    }

    /** */
    @Test
    public void testJoinCleanNode() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.context().globalMetastorage().write("key", "value");

        Thread.sleep(150L); // Remove later.

        IgniteEx newNode = startGrid(1);

        assertEquals("value", newNode.context().globalMetastorage().read("key"));

        assertGlobalMetastoragesAreEqual(ignite, newNode);
    }


    /** */
    @Test
    public void testJoinCleanNodeFullData() throws Exception {
        System.setProperty(IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, "0");

        try {
            IgniteEx ignite = startGrid(0);

            ignite.cluster().active(true);

            ignite.context().globalMetastorage().write("key1", "value1");

            ignite.context().globalMetastorage().write("key2", "value2");

            Thread.sleep(150L); // Remove later.

            IgniteEx newNode = startGrid(1);

            assertEquals("value1", newNode.context().globalMetastorage().read("key1"));

            assertEquals("value2", newNode.context().globalMetastorage().read("key2"));

            assertGlobalMetastoragesAreEqual(ignite, newNode);
        }
        finally {
            System.clearProperty(IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES);
        }
    }

    /** */
    protected DistributedMetaStorage metastorage(int i) {
        return grid(i).context().globalMetastorage();
    }

    /** */
    protected void assertGlobalMetastoragesAreEqual(IgniteEx ignite1, IgniteEx ignite2) throws Exception {
        DistributedMetaStorage globalMetastorage1 = ignite1.context().globalMetastorage();

        DistributedMetaStorage globalMetastorage2 = ignite2.context().globalMetastorage();

        assertEquals(U.<Object>field(globalMetastorage1, "ver"), U.field(globalMetastorage2, "ver"));

        Object histCache1 = U.field(globalMetastorage1, "histCache");

        Object histCache2 = U.field(globalMetastorage2, "histCache");

        assertEquals(histCache1, histCache2);

        Method fullDataMtd = U.findNonPublicMethod(DistributedMetaStorageImpl.class, "fullData");

        Object[] fullData1 = (Object[])fullDataMtd.invoke(globalMetastorage1);

        Object[] fullData2 = (Object[])fullDataMtd.invoke(globalMetastorage2);

        assertEqualsCollections(Arrays.asList(fullData1), Arrays.asList(fullData2));
    }
}
