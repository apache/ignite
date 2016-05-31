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

package org.apache.ignite.internal.processors.cache.datastructures.partitioned;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteAtomicsAbstractTest;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.Collection;

/**
 * The test will fall if deadlock detected.
 */
public class IgniteCountDownLatchDeadlockTest extends IgniteAtomicsAbstractTest {
    /**
     * Client flag
     */
    private boolean isClient = false;

    /** {@inheritDoc} */
    @Override protected CacheMode atomicsCacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        cfg.setClientMode(isClient);
        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatch() throws Exception {
        checkTopology(3);
        isClient = true;
        Ignite client = startGrid(4);
        ClusterGroup srvsGrp = client.cluster().forServers();

        int numOfSrvs = srvsGrp.nodes().size();

        client.destroyCache("testCache");
        IgniteCache<Object, Object> cache = client.createCache("testCache");

        for(ClusterNode node: srvsGrp.nodes())
            cache.put(String.valueOf(node.id()), 0);

        for(int i=0; i<500; i++) {
            IgniteCountDownLatch latch1 = createLatch1(client, numOfSrvs);
            IgniteCountDownLatch latch2 = createLatch2(client, numOfSrvs);

            client.compute(srvsGrp).broadcast(new IgniteRunnableJob(latch1, latch2, i));
            assertTrue(latch2.await(10000));
        }


    }

    /**
     * @param client Ignite client.
     * @param numOfSrvs Number of server nodes.
     * @return Ignite latch.
     */
    private IgniteCountDownLatch createLatch1(Ignite client, int numOfSrvs) {
        return client.countDownLatch(
                "testName1", // Latch name.
                numOfSrvs,          // Initial count.
                true,        // Auto remove, when counter has reached zero.
                true         // Create if it does not exist.
            );
    }

    /**
     * @param client Ignite client.
     * @param numOfSrvs Number of server nodes.
     * @return Ignite latch.
     */
    private IgniteCountDownLatch createLatch2(Ignite client, int numOfSrvs) {
        return client.countDownLatch(
                "testName2", // Latch name.
                numOfSrvs,          // Initial count.
                true,        // Auto remove, when counter has reached zero.
                true         // Create if it does not exist.
            );
    }

    /**
     * Ignite job
     */
    public class IgniteRunnableJob implements IgniteRunnable {

        /**
         * Ignite.
         */
        @IgniteInstanceResource
        Ignite igniteInstance;

        /**
         * Number of iteration.
         */
        protected final int iteration;

        /**
         * Ignite latch 1.
         */
        private final IgniteCountDownLatch latch1;

        /**
         * Ignite latch 2.
         */
        private final IgniteCountDownLatch latch2;

        /**
         * @param latch1 Ignite latch 1.
         * @param latch2 Ignite latch 2.
         * @param iteration Number of iteration.
         */
        public IgniteRunnableJob(IgniteCountDownLatch latch1, IgniteCountDownLatch latch2, int iteration) {
            this.iteration = iteration;
            this.latch1 = latch1;
            this.latch2 = latch2;
        }

        /**
         * @return Ignite latch.
         */
        IgniteCountDownLatch createLatch1() {
            return latch1;
        }

        /**
         * @return Ignite latch.
         */
        IgniteCountDownLatch createLatch2() {
            return latch2;
        }

        /** {@inheritDoc} */
        @Override
        public void run() {

            IgniteCountDownLatch latch1 = createLatch1();
            IgniteCountDownLatch latch2 = createLatch2();

            IgniteCache<Object, Object> cache = igniteInstance.cache("testCache");

            for(ClusterNode node:igniteInstance.cluster().forServers().nodes()) {
                Integer val = (Integer) cache.get(String.valueOf(node.id()));
                assertEquals(val, (Integer)iteration);
            }

            latch1.countDown();

            assertTrue(latch1.await(10000));

            cache.put(getUID(), (iteration + 1));

            latch2.countDown();

        }

        /**
         * @return Node UUID as string.
         */
        String getUID() {
            String id = "";
            Collection<ClusterNode> nodes = igniteInstance.cluster().forLocal().nodes();
            for (ClusterNode node : nodes) {
                if (node.isLocal())
                    id = String.valueOf(node.id());
            }
            return id;
        }

        /**
         * @return Ignite.
         */
        public Ignite igniteInstance() {
            return igniteInstance;
        }
    }
}
