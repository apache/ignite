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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.META_STORAGE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;

/**
 * Test for {@link DistributedMetaStorageImpl} with enabled persistence.
 */
public class DistributedMetaStoragePersistentTest extends DistributedMetaStorageTest {
    /** {@inheritDoc} */
    @Override protected boolean isPersistent() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void before() throws Exception {
        super.before();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override public void after() throws Exception {
        super.after();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestart() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.context().distributedMetastorage().write("key", "value");

        stopGrid(0);

        ignite = startGrid(0);

        ignite.cluster().active(true);

        assertEquals("value", ignite.context().distributedMetastorage().read("key"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinDirtyNode() throws Exception {
        IgniteEx ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().active(true);

        ignite.context().distributedMetastorage().write("key1", "value1");

        stopGrid(1);

        stopGrid(0);

        ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.context().distributedMetastorage().write("key2", "value2");

        IgniteEx newNode = startGrid(1);

        assertEquals("value1", newNode.context().distributedMetastorage().read("key1"));

        assertEquals("value2", newNode.context().distributedMetastorage().read("key2"));

        assertDistributedMetastoragesAreEqual(ignite, newNode);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, value = "0")
    public void testJoinDirtyNodeFullData() throws Exception {
        IgniteEx ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().active(true);

        ignite.context().distributedMetastorage().write("key1", "value1");

        stopGrid(1);

        stopGrid(0);

        ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.context().distributedMetastorage().write("key2", "value2");

        ignite.context().distributedMetastorage().write("key3", "value3");

        IgniteEx newNode = startGrid(1);

        assertEquals("value1", newNode.context().distributedMetastorage().read("key1"));

        assertEquals("value2", newNode.context().distributedMetastorage().read("key2"));

        assertEquals("value3", newNode.context().distributedMetastorage().read("key3"));

        assertDistributedMetastoragesAreEqual(ignite, newNode);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinNodeWithLongerHistory() throws Exception {
        IgniteEx ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().active(true);

        ignite.context().distributedMetastorage().write("key1", "value1");

        stopGrid(1);

        ignite.context().distributedMetastorage().write("key2", "value2");

        stopGrid(0);

        ignite = startGrid(1);

        startGrid(0);

        awaitPartitionMapExchange();

        assertEquals("value1", ignite.context().distributedMetastorage().read("key1"));

        assertEquals("value2", ignite.context().distributedMetastorage().read("key2"));

        assertDistributedMetastoragesAreEqual(ignite, grid(0));
    }

    /**
     * @throws Exception If failed.
     */
    @Test @Ignore
    @WithSystemProperty(key = IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, value = "0")
    public void testJoinNodeWithoutEnoughHistory() throws Exception {
        IgniteEx ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().active(true);

        ignite.context().distributedMetastorage().write("key1", "value1");

        stopGrid(1);

        ignite.context().distributedMetastorage().write("key2", "value2");

        ignite.context().distributedMetastorage().write("key3", "value3");

        stopGrid(0);

        ignite = startGrid(1);

        startGrid(0);

        awaitPartitionMapExchange();

        assertEquals("value1", ignite.context().distributedMetastorage().read("key1"));

        assertEquals("value2", ignite.context().distributedMetastorage().read("key2"));

        assertEquals("value3", ignite.context().distributedMetastorage().read("key3"));

        assertDistributedMetastoragesAreEqual(ignite, grid(0));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNamesCollision() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCacheDatabaseSharedManager dbSharedMgr = ignite.context().cache().context().database();

        MetaStorage locMetastorage = dbSharedMgr.metaStorage();

        DistributedMetaStorage distributedMetastorage = ignite.context().distributedMetastorage();

        dbSharedMgr.checkpointReadLock();

        try {
            locMetastorage.write("key", "localValue");
        }
        finally {
            dbSharedMgr.checkpointReadUnlock();
        }

        distributedMetastorage.write("key", "globalValue");

        dbSharedMgr.checkpointReadLock();

        try {
            assertEquals("localValue", locMetastorage.read("key"));
        }
        finally {
            dbSharedMgr.checkpointReadUnlock();
        }

        assertEquals("globalValue", distributedMetastorage.read("key"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, value = "0")
    public void testWrongStartOrder1() throws Exception {
        int cnt = 4;

        startGridsMultiThreaded(cnt);

        grid(0).cluster().active(true);

        metastorage(2).write("key1", "value1");

        stopGrid(2);

        metastorage(1).write("key2", "value2");

        stopGrid(1);

        metastorage(0).write("key3", "value3");

        stopGrid(0);

        metastorage(3).write("key4", "value4");

        stopGrid(3);

        for (int i = 0; i < cnt; i++)
            startGrid(i);

        awaitPartitionMapExchange();

        for (int i = 1; i < cnt; i++)
            assertDistributedMetastoragesAreEqual(grid(0), grid(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, value = "0")
    public void testWrongStartOrder2() throws Exception {
        int cnt = 6;

        startGridsMultiThreaded(cnt);

        grid(0).cluster().active(true);

        metastorage(4).write("key1", "value1");

        stopGrid(4);

        metastorage(3).write("key2", "value2");

        stopGrid(3);

        metastorage(0).write("key3", "value3");

        stopGrid(0);

        stopGrid(2);

        metastorage(1).write("key4", "value4");

        stopGrid(1);

        metastorage(5).write("key5", "value5");

        stopGrid(5);

        startGrid(1);

        startGrid(0);

        stopGrid(1);

        for (int i = 1; i < cnt; i++)
            startGrid(i);

        awaitPartitionMapExchange();

        for (int i = 1; i < cnt; i++)
            assertDistributedMetastoragesAreEqual(grid(0), grid(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, value = "0")
    public void testWrongStartOrder3() throws Exception {
        int cnt = 5;

        startGridsMultiThreaded(cnt);

        grid(0).cluster().active(true);

        metastorage(3).write("key1", "value1");

        stopGrid(3);

        stopGrid(0);

        metastorage(2).write("key2", "value2");

        stopGrid(2);

        metastorage(1).write("key3", "value3");

        stopGrid(1);

        metastorage(4).write("key4", "value4");

        stopGrid(4);

        startGrid(1);

        startGrid(0);

        stopGrid(1);

        for (int i = 1; i < cnt; i++)
            startGrid(i);

        awaitPartitionMapExchange();

        for (int i = 1; i < cnt; i++)
            assertDistributedMetastoragesAreEqual(grid(0), grid(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES, value = "0")
    public void testWrongStartOrder4() throws Exception {
        int cnt = 6;

        startGridsMultiThreaded(cnt);

        grid(0).cluster().active(true);

        metastorage(4).write("key1", "value1");

        stopGrid(4);

        stopGrid(0);

        metastorage(3).write("key2", "value2");

        stopGrid(3);

        metastorage(2).write("key3", "value3");

        stopGrid(2);

        metastorage(1).write("key4", "value4");

        stopGrid(1);

        metastorage(5).write("key5", "value5");

        stopGrid(5);

        startGrid(2);

        startGrid(0);

        stopGrid(2);

        for (int i = 1; i < cnt; i++)
            startGrid(i);

        awaitPartitionMapExchange();

        for (int i = 1; i < cnt; i++)
            assertDistributedMetastoragesAreEqual(grid(0), grid(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInactiveClusterWrite() throws Exception {
        startGrid(0);

        metastorage(0).write("key", "value");

        assertEquals("value", metastorage(0).read("key"));

        metastorage(0).remove("key");

        assertNull(metastorage(0).read("key"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateActivateRestart() throws Exception {
        startGrid(0);

        grid(0).cluster().active(true);

        grid(0).cluster().active(false);

        metastorage(0).write("key", "value");

        grid(0).cluster().active(true);

        stopGrid(0);

        startGrid(0);

        assertEquals("value", metastorage(0).read("key"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test @SuppressWarnings("ThrowableNotThrown")
    public void testConflictingData() throws Exception {
        IgniteEx igniteEx = startGrid(0);

        igniteEx.cluster().baselineAutoAdjustEnabled(false);

        startGrid(1);

        grid(0).cluster().active(true);

        stopGrid(0);

        metastorage(1).write("key", "value1");

        stopGrid(1);

        startGrid(0);

        grid(0).cluster().active(true);

        metastorage(0).write("key", "value2");

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> startGrid(1),
            IgniteSpiException.class,
            "Joining node has conflicting distributed metastorage data"
        );
    }

    /** */
    @Test
    @Ignore("This optimization is not implemented yet")
    public void testVerFromDiscoveryClusterData() throws Exception {
        startGrid(0);

        assumeThat(grid(0).context().config().getDiscoverySpi(), is(instanceOf(TcpDiscoverySpi.class)));

        startGrid(1).cluster().active(true);

        metastorage(0).write("key0", "value0");
        metastorage(0).write("key1", "value1");

        stopGrid(0);

        metastorage(1).write("key2", "value2");

        stopGrid(1);

        startGrid(0);

        TcpDiscoverySpi spi = (TcpDiscoverySpi)grid(0).context().config().getDiscoverySpi();

        DiscoverySpiDataExchange exchange = GridTestUtils.getFieldValue(spi, TcpDiscoverySpi.class, "exchange");

        List<Map<Integer, Serializable>> dataBags = new ArrayList<>();

        spi.setDataExchange(new DiscoverySpiDataExchange() {
            @Override public DiscoveryDataBag collect(DiscoveryDataBag dataBag) {
                dataBags.add(dataBag.joiningNodeData());

                return exchange.collect(dataBag);
            }

            @Override public void onExchange(DiscoveryDataBag dataBag) {
                exchange.onExchange(dataBag);
            }
        });

        startGrid(1);

        assertEquals(1, dataBags.size());

        byte[] joiningNodeDataMarshalled = (byte[])dataBags.get(0).get(META_STORAGE.ordinal());

        assertNotNull(joiningNodeDataMarshalled);

        Object joiningNodeData = JdkMarshaller.DEFAULT.unmarshal(joiningNodeDataMarshalled, U.gridClassLoader());

        Object[] hist = GridTestUtils.getFieldValue(joiningNodeData, "hist");

        assertEquals(1, hist.length);
    }

    /** */
    @Test
    public void testLongKey() throws Exception {
        startGrid(0).cluster().state(ClusterState.ACTIVE);

        String l10 = "1234567890";
        String longKey = l10 + l10 + l10 + l10 + l10 + l10 + l10;

        metastorage(0).write(longKey, "value");

        stopGrid(0);

        // Check that the value was actually persisted to the storage.

        startGrid(0);

        assertEquals("value", metastorage(0).read(longKey));

        metastorage(0).remove(longKey);

        stopGrid(0);

        startGrid(0);

        assertNull(metastorage(0).read(longKey));
    }
}
