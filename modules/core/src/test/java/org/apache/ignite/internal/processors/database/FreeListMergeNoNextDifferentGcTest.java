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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
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

/**
 * Test {@link IgniteCache#clear} and {@link PagesList#mergeNoNext} ensuring that the optimization
 * by the C2 Jit compiler was done for the {@link PagesList.CutTail#run}.
 */
@RunWith(Parameterized.class)
public class FreeListMergeNoNextDifferentGcTest extends GridCommonAbstractTest {
    /** Name of Ignite atomic to inform job in remove server node about C2 compilation. */
    private static final String COMPILED_ATOMIC_NAME = "compiled";

    /** Signals once method under test is C2 Jit compiled in remote server node. */
    private final CountDownLatch c2JitCompiledLatch = new CountDownLatch(1);

    /** Signals that failure was detected in remote server node. */
    private final AtomicBoolean failed = new AtomicBoolean(false);

    /** Page size. */
    private int pageSize;

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

    /** */
    @Test
    public void testMergeNoNext() throws Exception {
        try (Ignite ignite = prepareCluster(jvmOpts,
            "org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList$CutTail::run")) {
            IgniteAtomicReference<Boolean> compiled = ignite.atomicReference(COMPILED_ATOMIC_NAME, false, true);

            IgniteFuture<Void> jobFut = ignite.compute().runAsync(new TestMergeNoNextJob(pageSize));

            assertTrue(c2JitCompiledLatch.await(getTestTimeout() / 2, TimeUnit.MILLISECONDS));

            compiled.set(true);

            jobFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

            assertFalse("cache.clear() failed", failed.get());
        }
    }

    /**
     * Starts server node in remote JVM with the JVM options passed.
     * <p>
     * Registers listeners to signal once the method passed is C2 Jit-compiled
     * and if cache clear fails in the server node.
     * <p>
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
            c2JitCompiledLatch::countDown));

        lsnrLog.registerListener(new CallbackExecutorLogListener(
            ".*CorruptedFreeListException: Failed to remove data by link.*",
            () -> failed.set(true)));

        IgniteConfiguration cfg = optimize(getConfiguration("remote-jvm-server"));

        new IgniteProcessProxy(cfg, lsnrLog, null, false) {
            @Override protected Collection<String> filteredJvmArgs() throws Exception {
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

        return startClientGrid("local-jvm-client");
    }

    /**
     * Test that {@link PagesList#mergeNoNext} is not broken called from the {@link IgniteCache#clear}.
     * <p>
     * Notes:
     * <ul>
     * <li> Insert rows of the same size (which is a little bit smaller than the page) so that free
     *      pages are accumulated in the single bucket of the freelist.
     * <li> Insert rows in reverse key order and use one partition. This ensures that the most recently
     *      inserted rows will be removed first during cache clear. This forces that freelist's bucket will be shrinked
     *      from the tail and {@link PagesList#mergeNoNext} will be called.
     * <li> To force the {@link PagesList.CutTail#run} C2 Jit compilation:<ul>
     *     <li> Remove several rows to have some pages in the reuse bucket.
     *     <li> Number of the reuse pages should be just above the {@link PagesListNodeIO} capacity.
     *          So that adding single row would remove tail page from the reuse bucket list (cutting of the bucket's tail).
     *          And removing of this row would add new tail page again.
     *     <li> Repeatedly insert and remove row generating the {@link PagesList.CutTail#run} calls until it's C2 compiled.</ul>
     * <li> Number of remaining rows should be also just above the {@link PagesListNodeIO} capacity. So that freelist bucket has
     *      more than one page.
     * <li> If the {@link PagesList.CutTail#run} is broken by the C2 Jit compiler
     *      (see <a href="https://issues.apache.org/jira/browse/IGNITE-17734">IGNITE-17734</a>) the subsequent call to
     *      {@link IgniteCache#clear} would fail two times with:<ul>
     *      <li>"Tail not found: 0" - just in the {@link PagesList.CutTail#run} during the first attempt to cut the freelist bucket tail.
     *      <li>"Tail not found: 844420635166727" (or other non-zero page id) - during the last row remove in {@code PagesList#updateTail},
     *          since bucket's tail link wasn't correctly updated and contains outdated value because of previous failure.</ul>
     * </ul>
     */
    private static class TestMergeNoNextJob implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        Ignite ignite;

        /** */
        private final int pageSize;

        /** */
        private TestMergeNoNextJob(int pageSize) {
            this.pageSize = pageSize;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteAtomicReference<Boolean> compiled = ignite.atomicReference(COMPILED_ATOMIC_NAME, false, true);

            IgniteCache<Object, Object> cache = ignite.createCache(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAffinity(new RendezvousAffinityFunction(false, 1)));

            int capacity = PagesListNodeIO.VERSIONS.forVersion(1).getCapacity(pageSize);

            int keyCnt = capacity * 2 + 1;

            int dataSize = pageSize - AbstractDataPageIO.MIN_DATA_PAGE_OVERHEAD;

            for (int i = keyCnt; i > 0; i--)
                cache.put(i, new byte[dataSize - 64]);

            for (int i = 1; i <= capacity; i++)
                cache.remove(i);

            while (!compiled.get()) {
                try {
                    cache.put(0, new byte[2 * dataSize]);

                    cache.remove(0);
                }
                catch (Exception e) {
                    compiled.set(true);
                }
            }

            cache.clear();
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

        pageSize = dsCfg.getPageSize() == 0 ? DataStorageConfiguration.DFLT_PAGE_SIZE : dsCfg.getPageSize();

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        IgniteProcessProxy.killAll();
    }
}
