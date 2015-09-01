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

package org.apache.ignite.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests multiple parallel jobs execution.
 */
@GridCommonTest(group = "Kernal Self")
public class GridMultipleJobsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int LOG_MOD = 100;

    /** */
    private static final int TEST_TIMEOUT = 60 * 1000;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1);
        startGrid(2);

        assertEquals(2, grid(1).cluster().nodes().size());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid(1);
        stopGrid(2);

        assertEquals(0, G.allGrids().size());
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        if (getTestGridName(1).equals(gridName))
            c.setCacheConfiguration(/* no configured caches */);
        else {
            CacheConfiguration cc = defaultCacheConfiguration();

            cc.setCacheMode(PARTITIONED);
            cc.setBackups(1);

            c.setCacheConfiguration(cc);
        }

        return c;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNotAffinityJobs() throws Exception {
        /* =========== Test properties =========== */
        int jobsNum = 5000;
        int threadNum = 10;

        runTest(jobsNum, threadNum, NotAffinityJob.class);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testAffinityJobs() throws Exception {
        /* =========== Test properties =========== */
        int jobsNum = 5000;
        int threadNum = 10;

        runTest(jobsNum, threadNum, AffinityJob.class);
    }

    /**
     * @param jobsNum Number of jobs.
     * @param threadNum Number of threads.
     * @param jobCls Job class.
     * @throws Exception If failed.
     */
    private void runTest(final int jobsNum, int threadNum, final Class<? extends IgniteCallable<Boolean>> jobCls)
        throws Exception {
        final Ignite ignite1 = grid(1);

        final CountDownLatch latch = new CountDownLatch(jobsNum);

        final AtomicInteger jobsCnt = new AtomicInteger();

        final AtomicInteger resCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                while (true) {
                    int cnt = jobsCnt.incrementAndGet();

                    if (cnt > jobsNum)
                        break;

                    IgniteCallable<Boolean> job;

                    try {
                        job = jobCls.newInstance();
                    }
                    catch (Exception e) {
                        throw new IgniteCheckedException("Could not instantiate a job.", e);
                    }

                    IgniteCompute comp = ignite1.compute().withAsync();

                    comp.call(job);

                    ComputeTaskFuture<Boolean> fut = comp.future();

                    if (cnt % LOG_MOD == 0)
                        X.println("Submitted jobs: " + cnt);

                    fut.listen(new CIX1<IgniteFuture<Boolean>>() {
                        @Override public void applyx(IgniteFuture<Boolean> f) {
                            try {
                                assert f.get();
                            }
                            finally {
                                latch.countDown();

                                long cnt = resCnt.incrementAndGet();

                                if (cnt % LOG_MOD == 0)
                                    X.println("Results count: " + cnt);
                            }
                        }
                    });
                }
            }
        }, threadNum, "TEST-THREAD");

        latch.await();
    }

    /**
     * Test not affinity job.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class NotAffinityJob implements IgniteCallable<Boolean> {
        /** */
        private static AtomicInteger cnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            int c = cnt.incrementAndGet();

            if (c % LOG_MOD == 0)
                X.println("Executed jobs: " + c);

            Thread.sleep(10);

            return true;
        }
    }

    /**
     * Test affinity routed job.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class AffinityJob implements IgniteCallable<Boolean> {
        /** */
        private static AtomicInteger cnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            int c = cnt.incrementAndGet();

            if (c % LOG_MOD == 0)
                X.println("Executed affinity jobs: " + c);

            Thread.sleep(10);

            return true;
        }

        /**
         * @return Affinity key.
         */
        @AffinityKeyMapped
        public String affinityKey() {
            return "key";
        }
    }
}