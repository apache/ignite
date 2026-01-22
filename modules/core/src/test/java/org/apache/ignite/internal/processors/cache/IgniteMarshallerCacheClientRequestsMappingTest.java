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

import java.lang.reflect.Constructor;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataResponseMessage;
import org.apache.ignite.internal.processors.cache.persistence.filename.SharedFileTree;
import org.apache.ignite.internal.processors.marshaller.MappingAcceptedMessage;
import org.apache.ignite.internal.processors.marshaller.MappingProposedMessage;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
import org.apache.ignite.internal.processors.marshaller.MissingMappingResponseMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVTS_CACHE;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/** */
public class IgniteMarshallerCacheClientRequestsMappingTest extends GridCommonAbstractTest {
    /** Waiting timeout. */
    private static final long AWAIT_PROCESSING_TIMEOUT_MS = 10_000L;

    /** Limited thread pool size. */
    private static final int LIMITED_SYSTEM_THREAD_POOL = 4;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** External class loader. */
    private static final ClassLoader extClsLdr = getExternalClassLoader();

    /** Person class name. */
    private static final String PERSON_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Person";

    /** Organization class name. */
    private static final String ORGANIZATION_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Organization";

    /** Address class name. */
    private static final String ADDRESS_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Address";

    /** Compute job result class name. */
    private static final String JOB_RESULT_CLASS_NAME_PREFIX = "org.apache.ignite.tests.p2p.compute.ResultV";

    /** Client work directory absolute path. */
    private String clntWorkDir;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cfg.isClientMode())
            cfg.setWorkDirectory(clntWorkDir);

        cfg.setClassLoader(extClsLdr);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setWriteSynchronizationMode(FULL_SYNC));
        cfg.setIncludeEventTypes(EVTS_CACHE);

        cfg.setSystemThreadPoolSize(LIMITED_SYSTEM_THREAD_POOL);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        clntWorkDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "clnt", true).getAbsolutePath();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        U.delete(new SharedFileTree(clntWorkDir).marshaller());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDiscoeryMarshallerDelayedWithOverfloodThreadPool() throws Exception {
        doTestMarshallingBinaryMappingsLoadedFromClient(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDiscoeryBinaryMetaDelayedWithOverfloodThreadPool() throws Exception {
        doTestMarshallingBinaryMappingsLoadedFromClient(false);
    }

    /**
     * @param receiveMetadataOnClientJoin If {@code true} than binary metadata will exist on the server node and loaded
     * by the client node on the node join exchange, otherwise it will be requested by client peer-2-peer though the TcpCommunicationSpi.
     * @throws Exception If fails.
     */
    private void doTestMarshallingBinaryMappingsLoadedFromClient(boolean receiveMetadataOnClientJoin) throws Exception {
        CountDownLatch delayMappingLatch = new CountDownLatch(1);
        AtomicInteger loadKeys = new AtomicInteger(100);
        CountDownLatch evtRcvLatch = new CountDownLatch(1);
        int initKeys = receiveMetadataOnClientJoin ? 10 : 0;

        IgniteEx srv1 = startGrid(0);

        TestRecordingCommunicationSpi.spi(srv1)
            .blockMessages((IgniteBiPredicate<ClusterNode, Message>)(node, msg) -> msg instanceof MissingMappingResponseMessage ||
                msg instanceof MetadataResponseMessage);

        // Load data pior to the client note starts, so the client will receive the binary metadata on the client node join.
        for (int i = 0; i < initKeys; i++)
            srv1.cache(DEFAULT_CACHE_NAME).put(i, createOrganization(extClsLdr, i));

        Ignite cl1 = startClientGrid(1,
            (UnaryOperator<IgniteConfiguration>)cfg -> cfg.setDiscoverySpi(new TcpDiscoverySpi() {
                @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
                    if (msg instanceof TcpDiscoveryCustomEventMessage) {
                        try {
                            TcpDiscoveryCustomEventMessage msg0 = (TcpDiscoveryCustomEventMessage)msg;
                            msg0.finishUnmarhal(marshaller(), U.gridClassLoader());

                            DiscoveryCustomMessage customMsg = GridTestUtils.unwrap(msg0.message());

                            if (customMsg instanceof MappingAcceptedMessage) {
                                MarshallerMappingItem item = GridTestUtils.getFieldValue(customMsg, "item");

                                if (item.className().equals(PERSON_CLASS_NAME) ||
                                    item.className().equals(ORGANIZATION_CLASS_NAME) ||
                                    item.className().equals(ADDRESS_CLASS_NAME)
                                ) {
                                    try {
                                        U.await(delayMappingLatch, AWAIT_PROCESSING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                                    }
                                    catch (Exception e) {
                                        fail("Mapping proposed message must be released.");
                                    }
                                }
                            }
                        }
                        catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                    }

                    super.startMessageProcess(msg);
                }
            }.setIpFinder(IP_FINDER)));

        awaitPartitionMapExchange();

        cl1.events().remoteListen(
            (IgniteBiPredicate<UUID, Event>)(uuid, evt) -> {
                info("Event [" + evt.shortDisplay() + ']');

                evtRcvLatch.countDown();

                return true;
            },
            t -> true,
            EVT_CACHE_OBJECT_PUT);

        // Flood system thread pool with cache events.
        GridTestUtils.runMultiThreadedAsync((Callable<Boolean>)() -> {
            int key;

            while ((key = loadKeys.decrementAndGet()) > initKeys && !Thread.currentThread().isInterrupted())
                srv1.cache(DEFAULT_CACHE_NAME).put(key, createOrganization(extClsLdr, key));

            return true;
        }, 8, "cache-adder-thread").get();

        assertTrue(GridTestUtils.waitForCondition(() -> TestRecordingCommunicationSpi.spi(srv1).hasBlockedMessages(),
            AWAIT_PROCESSING_TIMEOUT_MS));

        delayMappingLatch.countDown();

        assertTrue(U.await(evtRcvLatch, AWAIT_PROCESSING_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryMetaDelayedForComputeJobResult() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        startGrid(0);

        Ignite cl1 = startClientGrid(1, (UnaryOperator<IgniteConfiguration>)cfg ->
            cfg.setDiscoverySpi(new TcpDiscoverySpi() {
                @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
                    if (msg instanceof TcpDiscoveryCustomEventMessage) {
                        try {
                            TcpDiscoveryCustomEventMessage msg0 = (TcpDiscoveryCustomEventMessage)msg;
                            msg0.finishUnmarhal(marshaller(), U.gridClassLoader());

                            DiscoveryCustomMessage customMsg = GridTestUtils.unwrap(msg0.message());

                            if (customMsg instanceof MappingProposedMessage) {
                                MarshallerMappingItem item = GridTestUtils.getFieldValue(customMsg, "mappingItem");

                                if (item.className().contains(JOB_RESULT_CLASS_NAME_PREFIX)) {
                                    try {
                                        U.await(latch, AWAIT_PROCESSING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                                    }
                                    catch (Exception e) {
                                        fail("Exception must never be thrown: " + e.getMessage());
                                    }
                                }
                            }
                        }
                        catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                    }

                    super.startMessageProcess(msg);
                }
            }.setIpFinder(IP_FINDER)));

        AtomicInteger results = new AtomicInteger(4);

        // Flood system thread pool with task results.
        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync((Callable<Boolean>)() -> {
            int v;

            while ((v = results.decrementAndGet()) >= 0) {
                int v0 = v;

                Object ignore = cl1.compute().call(() -> createResult(extClsLdr, v0));
            }

            return true;
        }, LIMITED_SYSTEM_THREAD_POOL, "compute-thread");

        latch.countDown();

        fut.get(AWAIT_PROCESSING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * @param extClsLdr Class loader.
     * @param key Cache key.
     * @return Organization.
     * @throws Exception If failed.
     */
    private static Object createOrganization(ClassLoader extClsLdr, int key) throws Exception {
        Class<?> personCls = extClsLdr.loadClass(PERSON_CLASS_NAME);
        Class<?> addrCls = extClsLdr.loadClass(ADDRESS_CLASS_NAME);

        Constructor<?> personConstructor = personCls.getConstructor(String.class);
        Constructor<?> addrConstructor = addrCls.getConstructor(String.class, Integer.TYPE);
        Constructor<?> organizationConstructor = extClsLdr.loadClass(ORGANIZATION_CLASS_NAME)
            .getConstructor(String.class, personCls, addrCls);

        return organizationConstructor.newInstance("Organization " + key,
            personConstructor.newInstance("Persone name " + key),
            addrConstructor.newInstance("Street " + key, key));
    }

    /**
     * @param extClsLdr Class loader.
     * @param ver Class type version.
     * @return Result.
     * @throws Exception If fails.
     */
    public static Object createResult(ClassLoader extClsLdr, int ver) throws Exception {
        Class<?> resCls = extClsLdr.loadClass(JOB_RESULT_CLASS_NAME_PREFIX + ver);

        return resCls.getConstructor(int.class).newInstance(ver);
    }
}
