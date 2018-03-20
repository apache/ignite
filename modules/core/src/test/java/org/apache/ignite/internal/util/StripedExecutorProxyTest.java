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

package org.apache.ignite.internal.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;

/**
 * Striped Thread Pool disabling test
 */
public class StripedExecutorProxyTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.getProperties().remove(IgniteSystemProperties.IGNITE_STRIPED_POOL_DISABLED);
    }

    /**
     * Check the implementations of StripedExecutor
     *
     * @throws Exception If failed.
     */
    public void testStripedExecutorService() throws Exception {
        // Striped Pool should be enabled by default.
        checkStripedExecutorImplementation(false);

        System.setProperty(IgniteSystemProperties.IGNITE_STRIPED_POOL_DISABLED, "true");
        checkStripedExecutorImplementation(true);

        System.setProperty(IgniteSystemProperties.IGNITE_STRIPED_POOL_DISABLED, "false");
        checkStripedExecutorImplementation(false);
    }

    /**
     * Check that StripedExecutor is instance of StripedExecutorProxy if striped pool is disabled.
     *
     * @param stripedPoolDisabled Striped pool is disabled.
     * @throws Exception If failed.
     */
    private void checkStripedExecutorImplementation(boolean stripedPoolDisabled) throws Exception {
        try (Ignite ignite = Ignition.start(getConfiguration())) {
            StripedExecutor executor = ((IgniteKernal)ignite).context().getStripedExecutorService();

            if (stripedPoolDisabled)
                Assert.assertThat(executor, CoreMatchers.instanceOf(StripedExecutorProxy.class));
            else
                Assert.assertThat(executor, CoreMatchers.instanceOf(StripedExecutorImpl.class));
        }
    }

    /**
     * Check that tasks work in parallel if the striped pool is disabled.
     *
     * @throws Exception If failed.
     */
    public void testParallelExecution() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_STRIPED_POOL_DISABLED, "true");

        IgniteConfiguration cfg = getConfiguration();

        cfg.setStripedPoolSize(2);

        try (Ignite ignite = Ignition.start(cfg)) {
            StripedExecutor executor = ((IgniteKernal)ignite).context().getStripedExecutorService();

            int tasks = 2;

            CountDownLatch started = new CountDownLatch(tasks);

            for (int i = 0; i < tasks; i++)
                executor.execute(1, new SimpleTask(started, started));

            if (!started.await(5000, TimeUnit.MILLISECONDS))
                fail("Unable to start jobs in parallel.");
        }
    }

    /**
     * Check that StripedExecutorProxy's methods are implemented correctly.
     *
     * @throws Exception If failed.
     */
    public void testExecutorProperties() throws Exception {
        final int poolSize = 4;
        final int completed = 10;
        final int queueSize = 7;

        StripedExecutor executor = new StripedExecutorProxy(
            "sys2",
            "instance-name",
            poolSize,
            poolSize,
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.SYSTEM_POOL,
            (thread, t) -> {});

        // Simulate completed tasks.
        for (int i = 0; i < completed; i++)
            executor.execute(1, new SimpleTask(null, null));

        final CountDownLatch started = new CountDownLatch(poolSize);
        final CountDownLatch canStop = new CountDownLatch(1);

        // Simulate active tasks.
        for (int i = 0; i < poolSize; i++)
            executor.execute(1, new SimpleTask(started, canStop));

        // Simulate tasks which will be in the queue.
        for (int i = 0; i < queueSize; i++)
            executor.execute(1, new SimpleTask(null, null));

        if (!started.await(10000, TimeUnit.MILLISECONDS))
            fail("Failed to start all active jobs.");

        assertEquals("Check completed tasks", completed, executor.completedTasks());

        assertEquals("Check active stripes count", 1, executor.activeStripesCount());

        assertEquals("Check queue size", queueSize, executor.queueSize());

        assertEquals("Check stripes count", 1, executor.stripes());

        assertEquals("Check stripes active statuses count", 1, executor.stripesActiveStatuses().length);

        assertEquals("Check stripes active statuses", true, executor.stripesActiveStatuses()[0]);

        Assert.assertArrayEquals("Check stripes completed tasks", new long[] { completed }, executor.stripesCompletedTasks());

        Assert.assertArrayEquals("Check stripes queue sizes", new int[] { queueSize }, executor.stripesQueueSizes());

        canStop.countDown();

        executor.shutdown();

        executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
    }

    /** Task */
    private static class SimpleTask implements Runnable {
        /** */
        final CountDownLatch started;

        /** */
        final CountDownLatch canStop;

        /**
         * @param started Latch to synchronize started tasks.
         * @param canStop Latch to simulate task hangs.
         */
        SimpleTask(CountDownLatch started, CountDownLatch canStop) {
            this.started = started;
            this.canStop = canStop;
        }

        /** */
        @Override public void run() {
            if (started != null)
                started.countDown();

            if (canStop != null) {
                try {
                    canStop.await();
                }
                catch (InterruptedException e) {
                    // No-op.
                }
            }
        }
    }
}
