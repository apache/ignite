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

package org.apache.ignite.internal.processors.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.CallbackExecutorLogListener;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 * Test freelists ensuring that the optimization by the C2 Jit compilator was done.
 */
@RunWith(Parameterized.class)
//@WithSystemProperty(key = IGNITE_PAGES_LIST_STRIPES_PER_BUCKET, value = "1")
public class FreeListMergeNoNextDifferentGcTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS_COUNT = 5_000;

    private static final boolean LOCAL = false;

    /** Signals once method under test is C2 Jit compiled in server node. */
    CountDownLatch c2JitCompiled = new CountDownLatch(1);

    AtomicBoolean failed = new AtomicBoolean(false);
    IgniteAtomicLong stop;

    /** JVM options to start the server node in remote JVM. */
    @Parameterized.Parameter
    public List<String> jvmOpts;

    /** */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<List<String>> params() {
        ArrayList<List<String>> params = new ArrayList<>(List.of(
            List.of("-XX:+UseShenandoahGC"),
            List.of("-XX:+UseShenandoahGC", "-ea"),

            List.of("-XX:+UseG1GC"),
            List.of("-XX:+UseG1GC", "-ea")
        ));

        if (Runtime.version().feature() >= 17 || U.isLinux())
            addZgc(params);

        return params;
    }

    /**
     * Ensure that the PagesList$CutTail::run handler is not broken if C2 Jit-compiled.
     * With various garbage collectors and with assertions both turned on and off.
     */
    @Test
    public void testCutTail() throws Exception {
        try (Ignite ignite = prepareCluster(jvmOpts,
                "org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList$CutTail::run")) {

            TestMergeNoNextJob job = new TestMergeNoNextJob(ignite);
            job.run();

            stop = ignite.atomicLong("stop", // Atomic long name.
                    0, // Initial value.
                    true // Create if it does not exist.
            );

            IgniteFuture<Void> jobFut = ignite.compute().runAsync(new TestCutTailJob());

            if (!LOCAL)
                assertTrue(c2JitCompiled.await(getTestTimeout() / 2, TimeUnit.MILLISECONDS));

            stop.incrementAndGet();

            jobFut.get();

            IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME+"1");
            cache.clear();

            assertFalse("cache.clear() failed", failed.get());
        }
    }

    /**
     * Starts server node in remote JVM with the JVM options passed.
     * Registers listener to signal once the method passed is C2 Jit-compiled in server node.
     * Starts client node in local JVM.
     *
     * @param jvmOpts JVM options for the server node.
     * @param method Fully qualified name of method.
     * @return Client node.
     */
    private Ignite prepareCluster(List<String> jvmOpts, String method) throws Exception {
        CountDownLatch remoteJvmServerStarted = new CountDownLatch(1);

        ListeningTestLogger lsnrLog = new ListeningTestLogger(log);

        lsnrLog.registerListener(new CallbackExecutorLogListener(".*Topology snapshot \\[ver=1,.*",
                remoteJvmServerStarted::countDown));

        lsnrLog.registerListener(new CallbackExecutorLogListener(
                ".*Compiled method \\(c2\\).*" + method.replace("$", "\\$") + ".*",
                c2JitCompiled::countDown));

        lsnrLog.registerListener(new CallbackExecutorLogListener(
            ".*CorruptedFreeListException: Failed to remove data by link.*",
                () -> failed.set(true)));

        if (!LOCAL) {
            IgniteConfiguration cfg = optimize(getConfiguration("remote-jvm-server"));

            new IgniteProcessProxy(cfg, lsnrLog, null, false) {
                @Override
                protected Collection<String> filteredJvmArgs() throws Exception {
                    Collection<String> args = super.filteredJvmArgs();

                    args.remove("-ea");

                    args.add("-XX:+UnlockDiagnosticVMOptions");
                    args.add("-XX:PrintAssemblyOptions=intel");
                    args.add("-XX:CompileCommand=print," + method);

                    args.addAll(jvmOpts);

                    return args;
                }
            };

            remoteJvmServerStarted.await(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        else
            startGrid("local-server-node");

        return startClientGrid("local-jvm-client");
    }

    /**
     * Perfrom concurrent updates and deletes ensuring the CutTail called enough
     * times to invoke the C2 optimizing Jit compiler.
     */
    private static class TestCutTailJob implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        Ignite ignite;

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

            for (int i = 0; i < KEYS_COUNT; i++)
                cache.put(i, value());

            AtomicLong cnt = new AtomicLong();

            IgniteAtomicLong stop = ignite.atomicLong("stop",0,false);

            CyclicBarrier barrier = new CyclicBarrier(6);

            IgniteInternalFuture<Long> updateFut = runMultiThreadedAsync(() -> {
                while (stop.get() == 0) {
                    try {
                        barrier.await(100, TimeUnit.MILLISECONDS);

                        cache.put(key(), value());
                    }
                    catch (InterruptedException | BrokenBarrierException ignored) {
                        stop.incrementAndGet();

                        barrier.reset();
                    }
                    catch (TimeoutException ignored) {
                        // No-op.
                    }
                    catch (Exception ex) {
                        cnt.getAndIncrement();

                        ignite.log().warning(String.format("cnt=%d, ex=%s", cnt.get(), ex.getMessage()));

                        stop.incrementAndGet();
                        barrier.reset();
                    }
                }
            }, 24, "update");

            while (stop.get() == 0) {
                try {
                    cache.remove(key());
                } catch (Exception e) {
                    ignite.log().warning(" cache.remove()");
                }
            }

            barrier.reset();

            GridCacheProcessor cacheProc = ((IgniteEx)ignite).context().cache();
            AbstractFreeList list = (AbstractFreeList)cacheProc.context().database().freeList(null);

            logList(ignite, list);

            try {
                updateFut.get();

                ignite.log().info(" first cache.clear()");

                cache.clear();
            }
            catch (IgniteCheckedException e) {
                ignite.log().error(" first cache.clear()", e);

                throw new RuntimeException(e);
            }

            logList(ignite, list);

            ignite.log().info(" cache.size()=" + cache.size(CachePeekMode.ALL));
        }

        /** */
        private int key() {
            return ThreadLocalRandom.current().nextInt(KEYS_COUNT);
        }

        /** */
        private byte[] value() {
            return new byte[3*4096 + 3100];
//            return new byte[ThreadLocalRandom.current().nextInt(12000) + 3000];
        }
    }

    private static void logList(Ignite ignite, AbstractFreeList list) {
        PagesList.Stripe[] stripes = list.getBucket(255);
        StringBuilder sb = new StringBuilder();

        for (int stripe = 0; stripe < stripes.length; stripe++) {

            if (stripe > 0)
                sb.append(", ");

            sb.append(stripe);
            sb.append(": ");
            sb.append(stripes[stripe].tailId);
        }

        ignite.log().info(sb.toString());
    }

    private static class TestMergeNoNextJob implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        Ignite ignite;

        public TestMergeNoNextJob() {
        }

        public TestMergeNoNextJob(Ignite ignite) {
            this.ignite = ignite;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(
                    new CacheConfiguration<>(DEFAULT_CACHE_NAME+"1")
                            .setAffinity(new RendezvousAffinityFunction(false, 1024)));

            ignite.log().info(" cache.put 505 cycle");

            long failed = 0;
            long inserted = 0;
            int i = 0;
            while (inserted < 5000 && i < 5000) {
                try {
                    cache.put(i, value());
//                    cache.put(i, new byte[4096 + 3000]);
//                    cache.put(key(), value());

                    inserted++;
                }
                catch (Exception e) {
//                    if (i % 10 == 0) {
                    //System.out.printf(" cache.put(i), i=%d\n", i);
//                        ignite.log().warning(" cache.put(i), i=" + i);

                    failed++;
//                    }
                }

                i++;
            }

            ignite.log().info(" cache.size()=" + cache.size(CachePeekMode.ALL));
            ignite.log().info(" failed=" + failed);

//            ignite.log().info(" cache.put(506)");
//
//            try {
//                cache.put(506, new byte[850]);
//            }
//            catch (Exception e) {
//                //System.out.printf(" cache.put(506) - %s\n", e);
//                ignite.log().error(" cache.put(506)", e);
//            }
//
//            ignite.log().info(" cache.put(507)");
//            try {
//                cache.put(507, new byte[850]);
//            }
//            catch (Exception e) {
//                //System.out.printf(" cache.put(507) - %s\n", e);
//                ignite.log().error(" cache.put(507)", e);
//            }


//            ignite.log().info(" second cache.clear()");
//
//            try {
//                cache.clear();
//            }
//            catch (Exception e) {
//                //System.out.printf(" cache.clear() - %s\n", e);
//                ignite.log().error(" cache.clear()", e);
//
//                throw e;
//            }
        }
        /** */
        private int key() {
            return ThreadLocalRandom.current().nextInt(KEYS_COUNT);
        }

        private byte[] value() {
            return new byte[4096 + 3000];
//            return new byte[ThreadLocalRandom.current().nextInt(300) + 10];
        }

    }

    /**
     * Add test parameters for the ZGC garbage collector.
     */
    private static void addZgc(ArrayList<List<String>> params) {
        ArrayList<String> opts = new ArrayList<>();

        if (Runtime.version().feature() < 12)
            opts.add("-XX:+UnlockExperimentalVMOptions");

        opts.add("-XX:+UseZGC");
        params.add(new ArrayList<>(opts));

        opts.add("-ea");
        params.add(new ArrayList<>(opts));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(LOCAL_IP_FINDER));

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        int pageSize = dsCfg.getPageSize() == 0 ? DataStorageConfiguration.DFLT_PAGE_SIZE : dsCfg.getPageSize();

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(pageSize * 100L * KEYS_COUNT));

        cfg.setDataStorageConfiguration(dsCfg);

//        cfg.setMetricsLogFrequency(2000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        IgniteProcessProxy.killAll();
    }
}
