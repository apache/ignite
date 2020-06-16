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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for client requesting missing mappings from server nodes with and without server nodes failures.
 */
public class IgniteMarshallerCacheClientRequestsMappingOnMissTest extends GridCommonAbstractTest {
    /**
     * Need to point client node to a different working directory
     * to avoid reading marshaller mapping from FS and to force sending MissingMappingRequest.
     */
    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    /** */
    private static final AtomicInteger mappingReqsCounter = new AtomicInteger(0);

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cfg.isClientMode())
            cfg.setWorkDirectory(TMP_DIR);

        TcpDiscoverySpi disco = new TestTcpDiscoverySpi();
        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(REPLICATED);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanupMarshallerFileStore();

        mappingReqsCounter.set(0);
    }

    /**
     *
     */
    private void cleanupMarshallerFileStore() throws IOException {
        Path marshCache = Paths.get(TMP_DIR, DataStorageConfiguration.DFLT_MARSHALLER_PATH);

        for (File file : marshCache.toFile().listFiles())
            Files.delete(file.toPath());

        Files.deleteIfExists(marshCache);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRequestedMappingIsStoredInFS() throws Exception {
        Ignite srv1 = startGrid(0);

        Organization org = new Organization(1, "Microsoft", "One Microsoft Way Redmond, WA 98052-6399, USA");

        srv1.cache(DEFAULT_CACHE_NAME).put(1, org);

        Ignite cl1 = startClientGrid(1);

        cl1.cache(DEFAULT_CACHE_NAME).get(1);

        String clsName = Organization.class.getName();

        stopGrid(1);

        File[] files = Paths.get(TMP_DIR, DataStorageConfiguration.DFLT_MARSHALLER_PATH).toFile().listFiles();

        assertNotNull(TMP_DIR + "/marshaller directory should contain at least one file", files);

        boolean orgClsMarshalled = false;

        for (File f : files) {
            if (clsName.equals(new String(Files.readAllBytes(f.toPath())))) {
                orgClsMarshalled = true;
                break;
            }
        }

        assertTrue(clsName + " should be marshalled and stored to disk", orgClsMarshalled);
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoNodesDieOnRequest() throws Exception {
        Ignite srv1 = startGrid(0);

        replaceWithCountingMappingRequestListener(((GridKernalContext)U.field(srv1, "ctx")).io());

        Ignite srv2 = startGrid(1);

        replaceWithCountingMappingRequestListener(((GridKernalContext)U.field(srv2, "ctx")).io());

        Ignite srv3 = startGrid(2);

        replaceWithCountingMappingRequestListener(((GridKernalContext)U.field(srv3, "ctx")).io());

        srv3.cache(DEFAULT_CACHE_NAME).put(
            1, new Organization(1, "Microsoft", "One Microsoft Way Redmond, WA 98052-6399, USA"));

        Ignite cl1 = startClientGrid(4);

        cl1.cache(DEFAULT_CACHE_NAME).get(1);

        int result = mappingReqsCounter.get();

        assertEquals("Expected requests count is 1, actual is " + result, 1, result);
    }

    /**
     *
     */
    @Test
    public void testOneNodeDiesOnRequest() throws Exception {
        CountDownLatch nodeStopLatch = new CountDownLatch(1);

        Ignite srv1 = startGrid(0);

        replaceWithStoppingMappingRequestListener(
            ((GridKernalContext)U.field(srv1, "ctx")).io(), 0, nodeStopLatch);

        Ignite srv2 = startGrid(1);

        replaceWithCountingMappingRequestListener(((GridKernalContext)U.field(srv2, "ctx")).io());

        Ignite srv3 = startGrid(2);

        replaceWithCountingMappingRequestListener(((GridKernalContext)U.field(srv3, "ctx")).io());

        srv3.cache(DEFAULT_CACHE_NAME).put(
            1, new Organization(1, "Microsoft", "One Microsoft Way Redmond, WA 98052-6399, USA"));

        Ignite cl1 = startClientGrid(4);

        cl1.cache(DEFAULT_CACHE_NAME).get(1);

        nodeStopLatch.await(5_000, TimeUnit.MILLISECONDS);

        int result = mappingReqsCounter.get();

        assertEquals("Expected requests count is 2, actual is " + result, 2, result);
    }

    /**
     *
     */
    @Test
    public void testTwoNodesDieOnRequest() throws Exception {
        CountDownLatch nodeStopLatch = new CountDownLatch(2);

        Ignite srv1 = startGrid(0);

        replaceWithStoppingMappingRequestListener(
            ((GridKernalContext)U.field(srv1, "ctx")).io(), 0, nodeStopLatch);

        Ignite srv2 = startGrid(1);

        replaceWithStoppingMappingRequestListener(
            ((GridKernalContext)U.field(srv2, "ctx")).io(), 1, nodeStopLatch);

        Ignite srv3 = startGrid(2);

        replaceWithCountingMappingRequestListener(((GridKernalContext)U.field(srv3, "ctx")).io());

        srv3.cache(DEFAULT_CACHE_NAME).put(
            1, new Organization(1, "Microsoft", "One Microsoft Way Redmond, WA 98052-6399, USA"));

        Ignite cl1 = startClientGrid(4);

        cl1.cache(DEFAULT_CACHE_NAME).get(1);

        nodeStopLatch.await(5_000, TimeUnit.MILLISECONDS);

        int result = mappingReqsCounter.get();

        assertEquals("Expected requests count is 3, actual is " + result, 3, result);
    }

    /**
     *
     */
    @Test
    public void testAllNodesDieOnRequest() throws Exception {
        CountDownLatch nodeStopLatch = new CountDownLatch(3);

        Ignite srv1 = startGrid(0);

        replaceWithStoppingMappingRequestListener(
            ((GridKernalContext)U.field(srv1, "ctx")).io(), 0, nodeStopLatch);

        Ignite srv2 = startGrid(1);

        replaceWithStoppingMappingRequestListener(
            ((GridKernalContext)U.field(srv2, "ctx")).io(), 1, nodeStopLatch);

        Ignite srv3 = startGrid(2);

        replaceWithStoppingMappingRequestListener(
            ((GridKernalContext)U.field(srv3, "ctx")).io(), 2, nodeStopLatch);

        srv3.cache(DEFAULT_CACHE_NAME).put(
            1, new Organization(1, "Microsoft", "One Microsoft Way Redmond, WA 98052-6399, USA"));

        Ignite cl1 = startClientGrid(4);

        try {
            cl1.cache(DEFAULT_CACHE_NAME).get(1);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        nodeStopLatch.await(5_000, TimeUnit.MILLISECONDS);

        int result = mappingReqsCounter.get();

        assertEquals("Expected requests count is 3, actual is " + result, 3, result);
    }

    /**
     *
     */
    private void replaceWithCountingMappingRequestListener(GridIoManager ioMgr) {
        GridMessageListener[] lsnrs = U.field(ioMgr, "sysLsnrs");

        final GridMessageListener delegate = lsnrs[GridTopic.TOPIC_MAPPING_MARSH.ordinal()];

        GridMessageListener wrapper = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                mappingReqsCounter.incrementAndGet();

                delegate.onMessage(nodeId, msg, plc);
            }
        };

        lsnrs[GridTopic.TOPIC_MAPPING_MARSH.ordinal()] = wrapper;
    }

    /**
     *
     */
    private void replaceWithStoppingMappingRequestListener(
        GridIoManager ioMgr,
        final int nodeIdToStop,
        final CountDownLatch latch
    ) {
        ioMgr.removeMessageListener(GridTopic.TOPIC_MAPPING_MARSH);

        ioMgr.addMessageListener(GridTopic.TOPIC_MAPPING_MARSH, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                new Thread(new Runnable() {
                    @Override public void run() {
                        mappingReqsCounter.incrementAndGet();

                        latch.countDown();

                        stopGrid(nodeIdToStop, true);
                    }
                }).start();
            }
        });
    }

    /**
     *
     */
    private static class Organization {
        /** */
        private final int id;

        /** */
        private final String name;

        /** */
        private final String addr;

        /**
         * @param id Id.
         * @param name Name.
         * @param addr Address.
         */
        Organization(int id, String name, String addr) {
            this.id = id;
            this.name = name;
            this.addr = addr;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", addr='" + addr + '\'' +
                    '}';
        }
    }

    /**
     * This implementation prevents client nodes from obtaining marshaller mapping data on discovery phase.
     *
     * It is needed to force client to request mapping from grid.
     */
    private static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void onExchange(DiscoveryDataPacket dataPacket, ClassLoader clsLdr) {
            if (locNode.isClient()) {
                Map<Integer, byte[]> cmnData = U.field(dataPacket, "commonData");

                cmnData.remove(GridComponent.DiscoveryDataExchangeType.MARSHALLER_PROC.ordinal());
            }

            super.onExchange(dataPacket, clsLdr);
        }
    }
}
