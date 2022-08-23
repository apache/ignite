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
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.marshaller.MappingProposedMessage;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
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
    private static final long AWAIT_PROCESSING_TIMEOUT_MS = 5000L;

    /** Limited thread pool size. */
    private static final int LIMITED_SYSTEM_THREAD_POOL = 2;

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

    /** Client work directory absolute path. */
    private String clntWorkDir;

    private static CountDownLatch latch = new CountDownLatch(1);

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cfg.isClientMode())
            cfg.setWorkDirectory(clntWorkDir);

        cfg.setClassLoader(getExternalClassLoader());

        if (cfg.isClientMode()) {
            cfg.setDiscoverySpi(new TcpDiscoverySpi() {
                @Override
                protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
                    if (msg instanceof TcpDiscoveryCustomEventMessage) {
                        try {
                            DiscoverySpiCustomMessage custom = ((TcpDiscoveryCustomEventMessage) msg).message(marshaller(), U.gridClassLoader());

                            if (custom instanceof CustomMessageWrapper) {
                                DiscoveryCustomMessage delegate = ((CustomMessageWrapper) custom).delegate();

                                if (delegate instanceof MappingProposedMessage) {
                                    MarshallerMappingItem item = GridTestUtils.getFieldValue(delegate, "mappingItem");

                                    if (item.className().contains("Result")) {
                                        try {
                                            latch.await();

                                            log.info("+++ UNLOCK MappingProposedMessage");
                                        } catch (Exception e) {
                                            log.error(e.getMessage(), e);
                                        }
                                    }
                                }
                            }
                        } catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                    }

                    super.startMessageProcess(msg);
                }
            }.setIpFinder(IP_FINDER));
        }
        else {
            cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));
        }

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

        Path path = Paths.get(clntWorkDir, DataStorageConfiguration.DFLT_MARSHALLER_PATH);

        for (File file : Objects.requireNonNull(path.toFile().listFiles()))
            Files.delete(file.toPath());

        Files.deleteIfExists(path);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRequestedMappingFromServerWithOverfloodThreadPool() throws Exception {
        int initialKeys = 10;
        AtomicInteger loadKeys = new AtomicInteger(1000);
        CountDownLatch processed = new CountDownLatch(1);
        IgniteEx srv1 = startGrid(0);

        for (int i = 0; i < initialKeys; i++)
            srv1.cache(DEFAULT_CACHE_NAME).put(i, createOrganization(extClsLdr, i));

        Ignite cl1 = startClientGrid(1);

        cl1.events().remoteListen(
            (IgniteBiPredicate<UUID, Event>)(uuid, evt) -> {
                info("Event [" + evt.shortDisplay() + ']');

                processed.countDown();

                return true;
            },
            t -> true,
            EVT_CACHE_OBJECT_PUT);

        // Flood system thread pool with cache events.
        GridTestUtils.runMultiThreadedAsync((Callable<Boolean>)() -> {
            int key;

            while ((key = loadKeys.decrementAndGet()) > initialKeys && !Thread.currentThread().isInterrupted())
                srv1.cache(DEFAULT_CACHE_NAME).put(key, createOrganization(extClsLdr, key));

            return true;
        }, 8, "cache-adder-thread").get();

        assertTrue(GridTestUtils.waitForCondition(() -> TestRecordingCommunicationSpi.spi(srv1).hasBlockedMessages(),
            AWAIT_PROCESSING_TIMEOUT_MS));

        TestRecordingCommunicationSpi.spi(srv1).stopBlock();

        assertTrue(U.await(processed, AWAIT_PROCESSING_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void dbg() throws Exception {
        IgniteEx srv1 = startGrid(0);

        Ignite cl1 = startClientGrid(1);

        AtomicInteger results = new AtomicInteger(2);

        // Flood system thread pool with task results.
        IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync((Callable<Boolean>)() -> {
            int v;
            while ((v = results.decrementAndGet()) >= 0) {
                int v0 = v;
                Object o = cl1.compute().call(() -> createResult(extClsLdr, v0));
                System.out.println("+++ call" + o);
            }

            return true;
        }, LIMITED_SYSTEM_THREAD_POOL, "compute-thread");

        U.sleep(1000);

        latch.countDown();

        fut.get();
    }


    /**
     * @param extClsLdr Class loader.
     * @param key Cache key.
     * @return Organization.
     * @throws Exception If failed.
     */
    public static Object createOrganization(ClassLoader extClsLdr, int key) throws Exception {
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

    public static Object createResult(ClassLoader extClsLdr, int v) throws Exception {
        Class<?> resCls = extClsLdr.loadClass("org.apache.ignite.tests.p2p.cache.Result" + v);

        Constructor<?> resConstructor = resCls.getConstructor(int.class);

        log.info("+++ createResult " + v);

        return resConstructor.newInstance(v);
    }
}
