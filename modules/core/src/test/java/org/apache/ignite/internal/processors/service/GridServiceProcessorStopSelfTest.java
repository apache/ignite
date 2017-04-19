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

package org.apache.ignite.internal.processors.service;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests that {@link GridServiceProcessor} completes deploy/undeploy futures during node stop.
 */
public class GridServiceProcessorStopSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopDuringDeployment() throws Exception {
        final CountDownLatch depLatch = new CountDownLatch(1);

        final CountDownLatch finishLatch = new CountDownLatch(1);

        final Ignite ignite = startGrid(0);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteServices svcs = ignite.services();

                IgniteFuture f = svcs.deployClusterSingletonAsync("myClusterSingletonService", new TestServiceImpl());

                depLatch.countDown();

                try {
                    f.get();
                }
                catch (IgniteException ignored) {
                    finishLatch.countDown();
                }
                finally {
                    finishLatch.countDown();
                }

                return null;
            }
        }, "deploy-thread");

        depLatch.await();

        Ignition.stopAll(true);

        boolean wait = finishLatch.await(15, TimeUnit.SECONDS);

        if (!wait)
            U.dumpThreads(log);

        assertTrue("Deploy future isn't completed", wait);

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopDuringHangedDeployment() throws Exception {
        final CountDownLatch depLatch = new CountDownLatch(1);

        final CountDownLatch finishLatch = new CountDownLatch(1);

        final IgniteEx node0 = startGrid(0);
        final IgniteEx node1 = startGrid(1);
        final IgniteEx node2 = startGrid(2);

        final IgniteCache<Object, Object> cache = node2.getOrCreateCache(new CacheConfiguration<Object, Object>("def")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        node0.services().deployNodeSingleton("myService", new TestServiceImpl());

        // Guarantee lock owner will never left topology unexpectedly.
        final Integer lockKey = keyForNode(node2.affinity("def"), new AtomicInteger(1),
            node2.cluster().localNode());

        // Lock to hold topology version undone.
        final Lock lock = cache.lock(lockKey);

        // Try to change topology once service has deployed.
        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                depLatch.await();

                node1.close();

                return null;
            }
        }, "top-change-thread");

        // Stop node on unstable topology.
        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                depLatch.await();

                Thread.sleep(1000);

                node0.close();

                finishLatch.countDown();

                return null;
            }
        }, "stopping-node-thread");

        assertNotNull(node0.services().service("myService"));

        // Freeze topology changing
        lock.lock();

        depLatch.countDown();

        boolean wait = finishLatch.await(15, TimeUnit.SECONDS);

        if (!wait)
            U.dumpThreads(log);

        assertTrue("Deploy future isn't completed", wait);

        fut.get();

        Ignition.stopAll(true);
    }

    /**
     * Simple map service.
     */
    public interface TestService {
        // No-op.
    }

    /**
     *
     */
    public class TestServiceImpl implements Service, TestService {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }
}
