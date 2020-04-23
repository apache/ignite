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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteBinaryMetadataUpdateNodeRestartTest extends GridCommonAbstractTest {
    /** */
    private static final String ATOMIC_CACHE = "atomicCache";

    /** */
    private static final String TX_CACHE = "txCache";

    /** */
    private static final int SRVS = 3;

    /** */
    private static final int CLIENTS = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setMarshaller(null);

        CacheConfiguration ccfg1 = cacheConfiguration(TX_CACHE, TRANSACTIONAL);
        CacheConfiguration ccfg2 = cacheConfiguration(ATOMIC_CACHE, ATOMIC);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setName(name);
        ccfg.setAtomicityMode(atomicityMode);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRestart() throws Exception {
        for (int i = 0; i < 10; i++) {
            log.info("Iteration: " + i);

            startGridsMultiThreaded(SRVS);

            startClientGrid(SRVS);

            final AtomicBoolean stop = new AtomicBoolean();

            try {
                IgniteInternalFuture<?> restartFut = GridTestUtils.runAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        while (!stop.get()) {
                            log.info("Start node.");

                            startClientGrid(SRVS + CLIENTS);

                            log.info("Stop node.");

                            stopGrid(SRVS + CLIENTS);
                        }

                        return null;
                    }
                }, "restart-thread");

                final AtomicInteger idx = new AtomicInteger();

                IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        int threadIdx = idx.getAndIncrement();

                        int node = threadIdx % (SRVS + CLIENTS);

                        Ignite ignite = ignite(node);

                        log.info("Started thread: " + ignite.name());

                        Thread.currentThread().setName("update-thread-" + threadIdx + "-" + ignite.name());

                        IgniteCache<Object, Object> cache1 = ignite.cache(ATOMIC_CACHE);
                        IgniteCache<Object, Object> cache2 = ignite.cache(TX_CACHE);

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        while (!stop.get()) {
                            try {
                                cache1.put(new TestClass1(true), create(rnd.nextInt(20) + 1));

                                cache1.invoke(new TestClass1(true), new TestEntryProcessor(rnd.nextInt(20) + 1));

                                cache2.put(new TestClass1(true), create(rnd.nextInt(20) + 1));

                                cache2.invoke(new TestClass1(true), new TestEntryProcessor(rnd.nextInt(20) + 1));
                            }
                            catch (CacheException | IgniteException e) {
                                log.info("Error: " + e);

                                if (X.hasCause(e, ClusterTopologyException.class)) {
                                    ClusterTopologyException cause = X.cause(e, ClusterTopologyException.class);

                                    if (cause.retryReadyFuture() != null)
                                        cause.retryReadyFuture().get();
                                }
                            }
                        }

                        return null;
                    }
                }, 10, "update-thread");

                U.sleep(5_000);

                stop.set(true);

                restartFut.get();

                fut.get();
            }
            finally {
                stop.set(true);

                stopAllGrids();
            }
        }
    }

    /**
     * @param id Class ID.
     * @return Test class instance.
     */
    private static Object create(int id) {
        switch (id) {
            case 1: return new TestClass1(true);

            case 2: return new TestClass2();

            case 3: return new TestClass3();

            case 4: return new TestClass4();

            case 5: return new TestClass5();

            case 6: return new TestClass6();

            case 7: return new TestClass7();

            case 8: return new TestClass8();

            case 9: return new TestClass9();

            case 10: return new TestClass10();

            case 11: return new TestClass11();

            case 12: return new TestClass12();

            case 13: return new TestClass13();

            case 14: return new TestClass14();

            case 15: return new TestClass15();

            case 16: return new TestClass16();

            case 17: return new TestClass17();

            case 18: return new TestClass18();

            case 19: return new TestClass19();

            case 20: return new TestClass20();
        }

        fail();

        return null;
    }

    /**
     *
     */
    static class TestEntryProcessor implements CacheEntryProcessor<Object, Object, Object> {
        /** */
        private int id;

        /**
         * @param id Value id.
         */
        public TestEntryProcessor(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry, Object... args) {
            entry.setValue(create(id));

            return null;
        }
    }

    /**
     *
     */
    static class TestClass1 {
        /** */
        int val;

        /**
         * @param setVal Set value flag.
         */
        public TestClass1(boolean setVal) {
            this.val = setVal ? ThreadLocalRandom.current().nextInt(10_000) : 0;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestClass1 that = (TestClass1)o;

            return val == that.val;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     *
     */
    static class TestClass2 {}

    /**
     *
     */
    static class TestClass3 {}

    /**
     *
     */
    static class TestClass4 {}

    /**
     *
     */
    static class TestClass5 {}

    /**
     *
     */
    static class TestClass6 {}

    /**
     *
     */
    static class TestClass7 {}

    /**
     *
     */
    static class TestClass8 {}

    /**
     *
     */
    static class TestClass9 {}

    /**
     *
     */
    static class TestClass10 {}

    /**
     *
     */
    static class TestClass11 {}

    /**
     *
     */
    static class TestClass12 {}

    /**
     *
     */
    static class TestClass13 {}

    /**
     *
     */
    static class TestClass14 {}

    /**
     *
     */
    static class TestClass15 {}

    /**
     *
     */
    static class TestClass16 {}

    /**
     *
     */
    static class TestClass17 {}

    /**
     *
     */
    static class TestClass18 {}

    /**
     *
     */
    static class TestClass19 {}

    /**
     *
     */
    static class TestClass20 {}
}
