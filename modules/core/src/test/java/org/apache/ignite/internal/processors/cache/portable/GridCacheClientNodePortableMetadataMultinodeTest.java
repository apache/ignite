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

package org.apache.ignite.internal.processors.cache.portable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgnitePortables;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableBuilder;
import org.apache.ignite.portable.PortableMetadata;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridCacheClientNodePortableMetadataMultinodeTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder).setForceServerMode(true);

        cfg.setMarshaller(new PortableMarshaller());

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientMetadataInitialization() throws Exception {
        startGrids(2);

        final AtomicBoolean stop = new AtomicBoolean();

        final ConcurrentHashSet<String> allTypes = new ConcurrentHashSet<>();

        IgniteInternalFuture<?> fut;

        try {
            // Update portable metadata concurrently with client nodes start.
            fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    IgnitePortables portables = ignite(0).portables();

                    IgniteCache<Object, Object> cache = ignite(0).cache(null).withKeepPortable();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < 1000; i++) {
                        log.info("Iteration: " + i);

                        String type = "portable-type-" + i;

                        allTypes.add(type);

                        for (int f = 0; f < 10; f++) {
                            PortableBuilder builder = portables.builder(type);

                            String fieldName = "f" + f;

                            builder.setField(fieldName, i);

                            cache.put(rnd.nextInt(0, 100_000), builder.build());

                            if (f % 100 == 0)
                                log.info("Put iteration: " + f);
                        }

                        if (stop.get())
                            break;
                    }

                    return null;
                }
            }, 5, "update-thread");
        }
        finally {
            stop.set(true);
        }

        client = true;

        startGridsMultiThreaded(2, 5);

        fut.get();

        assertFalse(allTypes.isEmpty());

        log.info("Expected portable types: " + allTypes.size());

        assertEquals(7, ignite(0).cluster().nodes().size());

        for (int i = 0; i < 7; i++) {
            log.info("Check metadata on node: " + i);

            boolean client = i > 1;

            assertEquals((Object)client, ignite(i).configuration().isClientMode());

            IgnitePortables portables = ignite(i).portables();

            Collection<PortableMetadata> metaCol = portables.metadata();

            assertEquals(allTypes.size(), metaCol.size());

            Set<String> names = new HashSet<>();

            for (PortableMetadata meta : metaCol) {
                assertTrue(names.add(meta.typeName()));

                assertNull(meta.affinityKeyFieldName());

                assertEquals(10, meta.fields().size());
            }

            assertEquals(allTypes.size(), names.size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailoverOnStart() throws Exception {
        startGrids(4);

        IgnitePortables portables = ignite(0).portables();

        IgniteCache<Object, Object> cache = ignite(0).cache(null).withKeepPortable();

        for (int i = 0; i < 1000; i++) {
            PortableBuilder builder = portables.builder("type-" + i);

            builder.setField("f0", i);

            cache.put(i, builder.build());
        }

        client = true;

        final CyclicBarrier barrier = new CyclicBarrier(6);

        final AtomicInteger startIdx = new AtomicInteger(4);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                barrier.await();

                Ignite ignite = startGrid(startIdx.getAndIncrement());

                assertTrue(ignite.configuration().isClientMode());

                log.info("Started node: " + ignite.name());

                return null;
            }
        }, 5, "start-thread");

        barrier.await();

        U.sleep(ThreadLocalRandom.current().nextInt(10, 100));

        for (int i = 0; i < 3; i++)
            stopGrid(i);

        fut.get();

        assertEquals(6, ignite(3).cluster().nodes().size());

        for (int i = 3; i < 7; i++) {
            log.info("Check metadata on node: " + i);

            boolean client = i > 3;

            assertEquals((Object) client, ignite(i).configuration().isClientMode());

            portables = ignite(i).portables();

            final IgnitePortables p0 = portables;

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    Collection<PortableMetadata> metaCol = p0.metadata();

                    return metaCol.size() == 1000;
                }
            }, getTestTimeout());

            Collection<PortableMetadata> metaCol = portables.metadata();

            assertEquals(1000, metaCol.size());

            Set<String> names = new HashSet<>();

            for (PortableMetadata meta : metaCol) {
                assertTrue(names.add(meta.typeName()));

                assertNull(meta.affinityKeyFieldName());

                assertEquals(1, meta.fields().size());
            }

            assertEquals(1000, names.size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientStartsFirst() throws Exception {
        client = true;

        Ignite ignite0 = startGrid(0);

        assertTrue(ignite0.configuration().isClientMode());

        client = false;

        Ignite ignite1 = startGrid(1);

        assertFalse(ignite1.configuration().isClientMode());

        IgnitePortables portables = ignite(1).portables();

        IgniteCache<Object, Object> cache = ignite(1).cache(null).withKeepPortable();

        for (int i = 0; i < 100; i++) {
            PortableBuilder builder = portables.builder("type-" + i);

            builder.setField("f0", i);

            cache.put(i, builder.build());
        }

        assertEquals(100, ignite(0).portables().metadata().size());
    }
}