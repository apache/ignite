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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** */
@RunWith(JUnit4.class)
public class DistributedMetaStorageTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
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

        startGrids(cnt);

        grid(0).cluster().active(true);

        for (int i = 0; i < cnt; i++) {
            String key = UUID.randomUUID().toString();

            String val = UUID.randomUUID().toString();

            grid(i).context().globalMetastorage().write(key, val);

            Thread.sleep(150L); // Remove later.

            for (int j = 0; j < cnt; j++)
                assertEquals(i + " " + j, val, grid(j).context().globalMetastorage().read(key));
        }
    }

    /** */
    @Test
    public void testListenersOnWrite() throws Exception {
        int cnt = 4;

        startGrids(cnt);

        grid(0).cluster().active(true);

        AtomicInteger strictCntr = new AtomicInteger();

        AtomicInteger predCntr = new AtomicInteger();

        for (int i = 0; i < cnt; i++) {
            DistributedMetaStorage metastorage = grid(i).context().globalMetastorage();

            metastorage.listen("key", (key, val) -> {
                assertEquals("value", val);

                strictCntr.incrementAndGet();
            });

            metastorage.listen(key -> key.startsWith("k"), (key, val) -> {
                assertEquals("value", val);

                predCntr.incrementAndGet();
            });
        }

        grid(0).context().globalMetastorage().write("key", "value");

        Thread.sleep(150L); // Remove later.

        assertEquals(cnt, strictCntr.get());

        assertEquals(cnt, predCntr.get());
    }

    /** */
    @Test
    public void testListenersOnRemove() throws Exception {
        int cnt = 4;

        startGrids(cnt);

        grid(0).cluster().active(true);

        grid(0).context().globalMetastorage().write("key", "value");

        Thread.sleep(150L); // Remove later.

        AtomicInteger strictCntr = new AtomicInteger();

        AtomicInteger predCntr = new AtomicInteger();

        for (int i = 0; i < cnt; i++) {
            DistributedMetaStorage metastorage = grid(i).context().globalMetastorage();

            metastorage.listen("key", (key, val) -> {
                assertNull(val);

                strictCntr.incrementAndGet();
            });

            metastorage.listen(key -> key.startsWith("k"), (key, val) -> {
                assertNull(val);

                predCntr.incrementAndGet();
            });
        }

        grid(0).context().globalMetastorage().remove("key");

        Thread.sleep(150L); // Remove later.

        assertEquals(cnt, strictCntr.get());

        assertEquals(cnt, predCntr.get());
    }

    /** */
    @Test
    public void testRestart() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.context().globalMetastorage().write("key", "value");

        Thread.sleep(150L); // Remove later.

        stopGrid(0);

        ignite = startGrid(0);

        ignite.cluster().active(true);

        assertEquals("value", ignite.context().globalMetastorage().read("key"));
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
    }

    /** */
    @Test
    public void testJoinDirtyNode() throws Exception {
        IgniteEx ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().active(true);

        ignite.context().globalMetastorage().write("key1", "value1");

        Thread.sleep(150L); // Remove later.

        stopGrid(1);

        stopGrid(0);

        ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.context().globalMetastorage().write("key2", "value2");

        Thread.sleep(150L); // Remove later.

        IgniteEx newNode = startGrid(1);

        assertEquals("value1", newNode.context().globalMetastorage().read("key1"));

        assertEquals("value2", newNode.context().globalMetastorage().read("key2"));
    }

    /** */
    @Test
    public void testRerunListenerOnRestore() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        DistributedMetaStorage metastorage = ignite.context().globalMetastorage();

        metastorage.listen("key", (key, val) -> {
            throw new RuntimeException();
        });

        metastorage.write("key", "value");

        Thread.sleep(150L); // Remove later.

        stopGrid(0);

        ignite = startGrid(0); // Add another listener somehow.

        ignite.cluster().active(true);

        assertEquals("value", ignite.context().globalMetastorage().read("key"));
    }

    /** */
    @Test
    public void testNamesCollision() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCacheDatabaseSharedManager dbSharedMgr = ignite.context().cache().context().database();

        MetaStorage locMetastorage = dbSharedMgr.metaStorage();

        DistributedMetaStorage globalMetastorage = ignite.context().globalMetastorage();

        dbSharedMgr.checkpointReadLock();

        try {
            locMetastorage.write("key", "localValue");
        }
        finally {
            dbSharedMgr.checkpointReadUnlock();
        }

        globalMetastorage.write("key", "globalValue");

        Thread.sleep(150L); // Remove later.

        dbSharedMgr.checkpointReadLock();

        try {
            assertEquals("localValue", locMetastorage.read("key"));
        }
        finally {
            dbSharedMgr.checkpointReadUnlock();
        }

        assertEquals("globalValue", globalMetastorage.read("key"));
    }
}