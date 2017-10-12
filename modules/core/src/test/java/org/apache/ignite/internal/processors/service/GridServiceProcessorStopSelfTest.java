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
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
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

                IgniteServices services = svcs.withAsync();

                services.deployClusterSingleton("myClusterSingletonService", new TestServiceImpl());

                depLatch.countDown();

                try {
                    services.future().get();
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
    }

    /**
     * @throws Exception If fails
     */
    public void testServiceDeploymentCancelationOnStop() throws Exception {
        final Ignite node = startGrid(0);

        final IgniteServices services = node.services();
        // Deploy some service.
        services.deploy(getServiceConfiguration("myService1"));

        //Stop node async, this will cancel the service #1.
        final IgniteInternalFuture<Boolean> stopAsync = GridTestUtils.runAsync(new GridPlainCallable<Boolean>() {
            @Override public Boolean call() throws Exception {
                node.close();

                return true;
            }
        }, "node-stopping-thread");

        // Wait for the service #1 cancellation during node stopping.
        // At this point node.stop process will be paused until svcCancelFinishLatch released.
        ServiceImpl.svcCancelStartLatch.await();

        final AtomicReference<IgniteFuture> queuedFuture = new AtomicReference<>();

        // Try to deploy another service.
        final IgniteInternalFuture<?> deployAsync = GridTestUtils.runAsync(new GridPlainCallable<Boolean>() {
            @Override public Boolean call() throws Exception {
                IgniteServices async = services.withAsync();

                async.deploy(getServiceConfiguration("myService2"));

                IgniteFuture<Object> future = async.future();

                queuedFuture.set(future);

                // Here, deployment future is added to queue and
                // then it will be cancelled when svcCancelFinishLatch be released.
                // So, we'll wait for queue cleaning and try to deploy one more service.
                try {
                    future.get();
                }
                catch (Exception ignore) {
                    // Noop.
                }

                // Normally, this should fail with some Exception as node is stopping right now.
                // But we face a deadlock here.
                for (int i = 0; i < 5; i++) {
                    try {
                        services.deploy(getServiceConfiguration("service3"));
                    }
                    catch (Exception ignore) {
                        // Noop.
                    }
                }

                return true;
            }
        }, "svc-deploy-thread");

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return queuedFuture.get() != null;
            }
        }, 3000);

        // Allow node to be stopped.
        ServiceImpl.svcCancelFinishLatch.countDown();

        // Wait for all service deployments have finished.
        boolean deployDone = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                IgniteFuture fut = queuedFuture.get();

                return fut != null && fut.isDone() && deployAsync.isDone();

            }
        }, 5000);

        assertTrue("Node stopping and service deployment processes falls into a deadlock.", deployDone);

        if (!deployDone)
            deployAsync.cancel();

        if (!stopAsync.isDone())
            stopAsync.cancel();
    }

    /** */
    private ServiceConfiguration getServiceConfiguration(String svcName) {
        ServiceConfiguration svcCfg = new ServiceConfiguration();
        svcCfg.setName(svcName);
        svcCfg.setService(new ServiceImpl());
        svcCfg.setTotalCount(1);

        return svcCfg;
    }

    /** Dummy Implementation. */
    static class ServiceImpl implements Service {
        /** */
        static final CountDownLatch svcCancelStartLatch = new CountDownLatch(1);

        /** */
        static final CountDownLatch svcCancelFinishLatch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            System.out.println("cancel service: " + ctx.executionId());
            try {
                svcCancelStartLatch.countDown();

                svcCancelFinishLatch.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            System.out.println("init service: " + ctx.executionId());
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op
        }
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
