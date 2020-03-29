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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Test for deployment of multiple configurations at a time.
 */
public class GridServiceProcessorBatchDeploySelfTest extends GridCommonAbstractTest {
    /** Number of services to be deployed. */
    private static final int NUM_SERVICES = 100;

    /** Number of nodes in the test cluster. */
    private static final int NUM_NODES = 4;

    /** Client node name. */
    private static final String CLIENT_NODE_NAME = "client";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < NUM_NODES; i++)
            startGrid(i);

        startClientGrid(CLIENT_NODE_NAME, getConfiguration(CLIENT_NODE_NAME));

        DummyService.reset();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployAll() throws Exception {
        Ignite client = grid(CLIENT_NODE_NAME);

        CountDownLatch latch = new CountDownLatch(NUM_SERVICES);

        List<ServiceConfiguration> cfgs = getConfigs(client.cluster().forServers().predicate(), NUM_SERVICES);

        subscribeExeLatch(cfgs, latch);

        client.services().deployAll(cfgs);

        assertTrue("Waiting for services deployment timed out.", latch.await(30, TimeUnit.SECONDS));

        assertDeployedServices(client, cfgs);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployAllAsync() throws Exception {
        Ignite client = grid(CLIENT_NODE_NAME);

        CountDownLatch latch = new CountDownLatch(NUM_SERVICES);

        List<ServiceConfiguration> cfgs = getConfigs(client.cluster().forServers().predicate(), NUM_SERVICES);

        subscribeExeLatch(cfgs, latch);

        IgniteFuture<Void> fut = client.services().deployAllAsync(cfgs);

        fut.get();

        assertTrue("Waiting for services deployment timed out.", latch.await(30, TimeUnit.SECONDS));

        assertDeployedServices(client, cfgs);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployAllTopologyChange() throws Exception {
        Ignite client = grid(CLIENT_NODE_NAME);

        final AtomicBoolean finished = new AtomicBoolean();

        IgniteInternalFuture<Object> topChangeFut = runTopChanger(finished);

        try {
            int numServices = 50;
            int batchSize = 5;

            CountDownLatch latch = new CountDownLatch(numServices);

            IgnitePredicate<ClusterNode> depPred = new TestPredicate(getTestIgniteInstanceName());

            List<ServiceConfiguration> cfgs = getConfigs(depPred, numServices);

            subscribeExeLatch(cfgs, latch);

            int from = 0;

            while (from < numServices) {
                int to = Math.min(numServices, from + batchSize);

                client.services().deployAllAsync(cfgs.subList(from, to)).get(5000);

                from = to;
            }

            assertTrue(latch.await(120, TimeUnit.SECONDS));

            assertDeployedServices(client, cfgs);
        }
        finally {
            finished.set(true);
        }

        topChangeFut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployAllTopologyChangeFail() throws Exception {
        final Ignite client = grid(CLIENT_NODE_NAME);

        final AtomicBoolean finished = new AtomicBoolean();

        IgniteInternalFuture<Object> topChangeFut = runTopChanger(finished);

        try {
            int numServices = 200;
            int batchSize = 5;

            CountDownLatch latch = new CountDownLatch(numServices);

            IgnitePredicate<ClusterNode> depPred = new TestPredicate(getTestIgniteInstanceName());

            List<ServiceConfiguration> cfgs = getConfigs(depPred, numServices);

            List<ServiceConfiguration> failingCfgs = new ArrayList<>();

            subscribeExeLatch(cfgs, latch);

            int from = 0;

            while (from < numServices) {
                int to = Math.min(numServices, from + batchSize);

                List<ServiceConfiguration> cfgsBatch = cfgs.subList(from, to);

                ServiceConfiguration failingCfg = cfgsBatch.get(0);

                failingCfg.setName(null);

                failingCfgs.add(failingCfg);

                try {
                    client.services().deployAllAsync(cfgsBatch).get(5000);

                    fail("Should never reach here.");
                }
                catch (ServiceDeploymentException e) {
                    assertEquals(1, e.getFailedConfigurations().size());

                    ServiceConfiguration actFailedCfg = copyService(e.getFailedConfigurations().iterator().next());

                    assertEquals(failingCfg, actFailedCfg);

                    latch.countDown();
                }

                from = to;
            }

            assertTrue(latch.await(120, TimeUnit.SECONDS));

            cfgs.removeAll(failingCfgs);

            assertDeployedServices(client, cfgs);
        }
        finally {
            finished.set(true);
        }

        topChangeFut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployAllFail() throws Exception {
        deployAllFail(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployAllAsyncFail() throws Exception {
        deployAllFail(true);
    }

    /**
     * @param async If {@code true}, then asynchronous method of deployment will be performed.
     * @throws Exception If failed.
     */
    private void deployAllFail(boolean async) throws Exception {
        Ignite client = grid(CLIENT_NODE_NAME);

        CountDownLatch latch = new CountDownLatch(NUM_SERVICES - 1);

        List<ServiceConfiguration> cfgs = getConfigs(client.cluster().forServers().predicate(), NUM_SERVICES);

        subscribeExeLatch(cfgs, latch);

        ServiceConfiguration failingCfg = cfgs.get(cfgs.size() - 1);

        failingCfg.setName(null);

        assertFailingDeploy(client, async, cfgs, failingCfg);

        assertTrue("Waiting for services deployment timed out.", latch.await(30, TimeUnit.SECONDS));

        assertDeployedServices(client, cfgs.subList(0, cfgs.size() - 1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClashingNames() throws Exception {
        Ignite client = grid(CLIENT_NODE_NAME);

        CountDownLatch latch = new CountDownLatch(NUM_SERVICES);

        List<ServiceConfiguration> cfgs = getConfigs(client.cluster().forServers().predicate(), NUM_SERVICES);

        subscribeExeLatch(cfgs, latch);

        List<ServiceConfiguration> fstBatch = cfgs.subList(0, NUM_SERVICES / 2);
        List<ServiceConfiguration> sndBatch = cfgs.subList(NUM_SERVICES / 4, NUM_SERVICES);

        IgniteFuture<Void> fstFut = client.services().deployAllAsync(fstBatch);
        IgniteFuture<Void> sndFut = client.services().deployAllAsync(sndBatch);

        fstFut.get();
        sndFut.get();

        assertTrue("Waiting for services deployment timed out.", latch.await(30, TimeUnit.SECONDS));

        assertDeployedServices(client, cfgs);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClashingNamesFail() throws Exception {
        Ignite client = grid(CLIENT_NODE_NAME);

        List<ServiceConfiguration> cfgs = getConfigs(client.cluster().forServers().predicate(), NUM_SERVICES);

        int numDepSvcs = NUM_SERVICES - 1;

        CountDownLatch latch = new CountDownLatch(numDepSvcs);

        List<ServiceConfiguration> fstBatch = cfgs.subList(0, NUM_SERVICES / 2);
        List<ServiceConfiguration> sndBatch = cfgs.subList(NUM_SERVICES / 4, NUM_SERVICES);

        subscribeExeLatch(cfgs, latch);

        IgniteFuture<Void> fut = client.services().deployAllAsync(fstBatch);

        ServiceConfiguration failingCfg = cfgs.get(NUM_SERVICES - 1);

        failingCfg.setName(null);

        assertFailingDeploy(client, false, sndBatch, failingCfg);

        fut.get();

        assertTrue("Waiting for services deployment timed out.", latch.await(30, TimeUnit.SECONDS));

        assertDeployedServices(client, cfgs.subList(0, numDepSvcs));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClashingNameDifferentConfig() throws Exception {
        Ignite client = grid(CLIENT_NODE_NAME);

        List<ServiceConfiguration> cfgs = getConfigs(client.cluster().forServers().predicate(), NUM_SERVICES);

        int numDepSvcs = NUM_SERVICES - 1;

        CountDownLatch latch = new CountDownLatch(numDepSvcs);

        List<ServiceConfiguration> fstBatch = cfgs.subList(0, NUM_SERVICES / 2);
        List<ServiceConfiguration> sndBatch = cfgs.subList(NUM_SERVICES / 4, NUM_SERVICES - 1);

        subscribeExeLatch(cfgs, latch);

        client.services().deployAll(fstBatch);

        ServiceConfiguration failingCfg = copyService(cfgs.get(NUM_SERVICES - 1));

        // Same name, different config.
        failingCfg.setName(fstBatch.get(0).getName());
        failingCfg.setTotalCount(fstBatch.get(0).getTotalCount() + 1);

        sndBatch.add(failingCfg);

        assertFailingDeploy(client, false, sndBatch, failingCfg);

        assertTrue("Waiting for services deployment timed out.", latch.await(30, TimeUnit.SECONDS));

        assertDeployedServices(client, cfgs.subList(0, numDepSvcs));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelAll() throws Exception {
        Ignite client = grid(CLIENT_NODE_NAME);

        List<ServiceConfiguration> cfgs = getConfigs(client.cluster().forServers().predicate(), NUM_SERVICES);

        CountDownLatch latch = new CountDownLatch(NUM_SERVICES);

        subscribeExeLatch(cfgs, latch);

        client.services().deployAll(cfgs);

        latch.await(30, TimeUnit.SECONDS);

        client.services().cancelAll();

        assertDeployedServices(client, Collections.<ServiceConfiguration>emptyList());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelAllAsync() throws Exception {
        Ignite client = grid(CLIENT_NODE_NAME);

        List<ServiceConfiguration> cfgs = getConfigs(client.cluster().forServers().predicate(), NUM_SERVICES);

        CountDownLatch latch = new CountDownLatch(NUM_SERVICES);

        subscribeExeLatch(cfgs, latch);

        client.services().deployAll(cfgs);

        latch.await(30, TimeUnit.SECONDS);

        IgniteFuture<Void> fut = client.services().cancelAllAsync();

        fut.get();

        assertDeployedServices(client, Collections.<ServiceConfiguration>emptyList());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelAllTopologyChange() throws Exception {
        IgniteEx client = grid(CLIENT_NODE_NAME);

        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-10021",
            client.context().service() instanceof GridServiceProcessor);

        int numServices = 500;

        List<ServiceConfiguration> cfgs = getConfigs(client.cluster().forServers().predicate(), numServices);

        CountDownLatch latch = new CountDownLatch(numServices);

        subscribeExeLatch(cfgs, latch);

        client.services().deployAll(cfgs);

        latch.await(30, TimeUnit.SECONDS);

        final AtomicBoolean finished = new AtomicBoolean();

        IgniteInternalFuture<Object> topChangeFut = runTopChanger(finished);

        List<String> names = new ArrayList<>();

        for (ServiceConfiguration cfg : cfgs)
            names.add(cfg.getName());

        try {
            int batchSize = 5;
            int from = 0;

            while (from < numServices) {
                int to = Math.min(numServices, from + batchSize);

                log.info("Trying to cancel services [" + from + ".." + to + ")");

                client.services().cancelAllAsync(names.subList(from, to)).get(5000);

                from = to;
            }

            assertDeployedServices(client, Collections.<ServiceConfiguration>emptyList());
        }
        finally {
            finished.set(true);
        }

        topChangeFut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelAllClashingNames() throws Exception {
        Ignite client = grid(CLIENT_NODE_NAME);

        List<ServiceConfiguration> cfgs = getConfigs(client.cluster().forServers().predicate(), NUM_SERVICES);

        CountDownLatch latch = new CountDownLatch(NUM_SERVICES);

        subscribeExeLatch(cfgs, latch);

        client.services().deployAll(cfgs);

        latch.await(30, TimeUnit.SECONDS);

        List<String> names = new ArrayList<>();

        for (ServiceConfiguration cfg : cfgs)
            names.add(cfg.getName());

        int batchSize = 5;
        int from = 0;

        while (from < NUM_SERVICES) {
            int to = Math.min(NUM_SERVICES, from + batchSize);

            List<String> toCancel = new ArrayList<>(names.subList(from, to));

            toCancel.add(toCancel.get(0));

            client.services().cancelAll(toCancel);

            from = to;
        }

        assertDeployedServices(client, Collections.<ServiceConfiguration>emptyList());
    }

    /**
     * @param client Client.
     * @param async If {@code true}, then async version of deploy method will be used.
     * @param cfgs Service configurations.
     * @param failingCfg Configuration of the failing service.
     * @throws Exception If failed.
     */
    private void assertFailingDeploy(Ignite client, boolean async, List<ServiceConfiguration> cfgs,
        ServiceConfiguration failingCfg) throws Exception {

        IgniteFuture<Void> fut = null;

        if (async)
            fut = client.services().deployAllAsync(cfgs);

        try {
            if (async)
                fut.get();
            else
                client.services().deployAll(cfgs);

            fail("Should never reach here.");
        }
        catch (ServiceDeploymentException e) {
            info("Expected exception: " + e.getMessage());

            Collection<ServiceConfiguration> expFails = Collections.singleton(failingCfg);

            Collection<ServiceConfiguration> actFails = e.getFailedConfigurations();

            // Some cfgs may be lazy. Construct ServiceConfiguration from them for comparison.
            Collection<ServiceConfiguration> actFailsCp = new ArrayList<>(actFails.size());

            for (ServiceConfiguration cfg : actFails)
                actFailsCp.add(copyService(cfg));

            assertEqualsCollections(expFails, actFailsCp);
        }
    }

    /**
     * @param cfg Config.
     * @return Copy of provided configuration.
     */
    private ServiceConfiguration copyService(ServiceConfiguration cfg) {
        ServiceConfiguration cfgCp = new ServiceConfiguration();

        cfgCp.setName(cfg.getName());

        cfgCp.setMaxPerNodeCount(cfg.getMaxPerNodeCount());

        cfgCp.setTotalCount(cfg.getTotalCount());

        cfgCp.setAffinityKey(cfg.getAffinityKey());

        cfgCp.setCacheName(cfg.getCacheName());

        cfgCp.setName(cfg.getName());

        cfgCp.setService(cfg.getService());

        cfgCp.setNodeFilter(cfg.getNodeFilter());

        return cfgCp;
    }

    /**
     * @param client Client Ignite instance.
     * @param expCfgs Configurations of services that are expected to be deployed.
     */
    private void assertDeployedServices(Ignite client, Collection<ServiceConfiguration> expCfgs) {
        Set<String> expNames = new HashSet<>();
        Set<String> actNames = new HashSet<>();

        for (ServiceConfiguration cfg : expCfgs)
            expNames.add(cfg.getName());

        for (ServiceDescriptor desc : client.services().serviceDescriptors())
            actNames.add(desc.name());

        assertEquals(expNames, actNames);
    }

    /**
     * @param nodePred Node predicate.
     * @param numServices Number of configurations to generate.
     * @return Generated services configurations.
     */
    private List<ServiceConfiguration> getConfigs(IgnitePredicate<ClusterNode> nodePred, int numServices) {
        List<ServiceConfiguration> cfgs = new ArrayList<>(numServices);

        for (int i = 0; i < numServices; i++) {
            String name = "testService-" + i;

            ServiceConfiguration cfg = new ServiceConfiguration();

            cfg.setName(name);
            cfg.setTotalCount(1);
            cfg.setMaxPerNodeCount(1);
            cfg.setService(new DummyService());
            cfg.setNodeFilter(nodePred);

            cfgs.add(cfg);
        }
        return cfgs;
    }

    /**
     * @param cfgs Configurations.
     * @param latch Latch.
     */
    private void subscribeExeLatch(List<ServiceConfiguration> cfgs, CountDownLatch latch) {
        for (ServiceConfiguration cfg : cfgs)
            DummyService.exeLatch(cfg.getName(), latch);
    }

    /**
     * @param finished Finished flag.
     * @throws Exception If failed.
     * @return Future.
     */
    private IgniteInternalFuture<Object> runTopChanger(final AtomicBoolean finished) throws Exception {
        return runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                String namePrefix = "extra-node-";

                int extraNodesNum = 3;

                while (!finished.get()) {
                    for (int i = 0; i < extraNodesNum; i++)
                        startGrid(namePrefix + i);

                    for (int i = 0; i < extraNodesNum; i++)
                        stopGrid(namePrefix + i);

                    awaitPartitionMapExchange();
                }

                return null;
            }
        });
    }

    /**
     * Test predicate.
     */
    private static class TestPredicate implements IgnitePredicate<ClusterNode> {
        /** */
        private final String namePrefix;

        /**
         * @param namePrefix Prefix to match instances name.
         */
        public TestPredicate(String namePrefix) {
            this.namePrefix = namePrefix;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            String gridName = node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME);

            assert gridName != null;

            return !node.isClient() && gridName.startsWith(namePrefix);
        }
    }
}
