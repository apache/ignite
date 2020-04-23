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

package org.apache.ignite.internal.processors.cache.binary;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridCacheClientNodeBinaryObjectMetadataMultinodeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        cfg.setMarshaller(new BinaryMarshaller());

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

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
    @Test
    public void testClientMetadataInitialization() throws Exception {
        startGrids(2);

        final AtomicBoolean stop = new AtomicBoolean();

        final GridConcurrentHashSet<String> allTypes = new GridConcurrentHashSet<>();

        IgniteInternalFuture<?> fut;

        try {
            // Update binary metadata concurrently with client nodes start.
            fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    IgniteBinary binaries = ignite(0).binary();

                    IgniteCache<Object, Object> cache = ignite(0).cache(DEFAULT_CACHE_NAME).withKeepBinary();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < 1000; i++) {
                        log.info("Iteration: " + i);

                        String type = "binary-type-" + i;

                        allTypes.add(type);

                        for (int f = 0; f < 10; f++) {
                            BinaryObjectBuilder builder = binaries.builder(type);

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

        startClientGridsMultiThreaded(2, 5);

        fut.get();

        assertFalse(allTypes.isEmpty());

        log.info("Expected binary types: " + allTypes.size());

        assertEquals(7, ignite(0).cluster().nodes().size());

        for (int i = 0; i < 7; i++) {
            log.info("Check metadata on node: " + i);

            boolean client = i > 1;

            assertEquals((Object)client, ignite(i).configuration().isClientMode());

            IgniteBinary binaries = ignite(i).binary();

            Collection<BinaryType> metaCol = binaries.types();

            assertEquals(allTypes.size(), metaCol.size());

            Set<String> names = new HashSet<>();

            for (BinaryType meta : metaCol) {
                info("Binary type: " + meta);

                assertTrue(names.add(meta.typeName()));

                assertNull(meta.affinityKeyFieldName());

                assertEquals(10, meta.fieldNames().size());
            }

            assertEquals(allTypes.size(), names.size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFailoverOnStart() throws Exception {
        startGrids(4);

        IgniteBinary binaries = ignite(0).binary();

        IgniteCache<Object, Object> cache = ignite(0).cache(DEFAULT_CACHE_NAME).withKeepBinary();

        for (int i = 0; i < 1000; i++) {
            BinaryObjectBuilder builder = binaries.builder("type-" + i);

            builder.setField("f0", i);

            cache.put(i, builder.build());
        }

        final CyclicBarrier barrier = new CyclicBarrier(6);

        final AtomicInteger startIdx = new AtomicInteger(4);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                barrier.await();

                Ignite ignite = startClientGrid(startIdx.getAndIncrement());

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

            binaries = ignite(i).binary();

            final IgniteBinary p0 = binaries;

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    Collection<BinaryType> metaCol = p0.types();

                    return metaCol.size() >= 1000;
                }
            }, getTestTimeout());

            Collection<BinaryType> metaCol = binaries.types();

            assertEquals(1000, metaCol.size());

            Set<String> names = new HashSet<>();

            for (BinaryType meta : metaCol) {
                assertTrue(names.add(meta.typeName()));

                assertNull(meta.affinityKeyFieldName());

                assertEquals(1, meta.fieldNames().size());
            }

            assertEquals(1000, names.size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientStartsFirst() throws Exception {
        final Ignite ignite0 = startClientGrid(0);

        assertTrue(ignite0.configuration().isClientMode());

        Ignite ignite1 = startGrid(1);

        assertFalse(ignite1.configuration().isClientMode());

        IgniteBinary binaries = ignite(1).binary();

        IgniteCache<Object, Object> cache = ignite(1).cache(DEFAULT_CACHE_NAME).withKeepBinary();

        for (int i = 0; i < 100; i++) {
            BinaryObjectBuilder builder = binaries.builder("type-" + i);

            builder.setField("f0", i);

            cache.put(i, builder.build());
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ignite0.binary().types().size() == 100;
            }
        }, 5000);

        assertEquals(100, ignite(0).binary().types().size());
    }
}
