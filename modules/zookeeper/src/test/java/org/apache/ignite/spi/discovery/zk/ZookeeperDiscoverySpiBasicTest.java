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

package org.apache.ignite.spi.discovery.zk;

import java.util.List;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class ZookeeperDiscoverySpiBasicTest extends GridCommonAbstractTest {
    /** */
    private TestingCluster zkCluster;

    /** */
    private static final boolean USE_TEST_CLUSTER = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        ZookeeperDiscoverySpi zkSpi = new ZookeeperDiscoverySpi();

        if (USE_TEST_CLUSTER) {
            assert zkCluster != null;

            zkSpi.setConnectString(zkCluster.getConnectString());
        }
        else
            zkSpi.setConnectString("localhost:2181");

        cfg.setDiscoverySpi(zkSpi);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setMarshaller(new JdkMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (USE_TEST_CLUSTER) {
            zkCluster = new TestingCluster(1);
            zkCluster.start();
        }

    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (zkCluster != null) {
            zkCluster.close();

            zkCluster = null;
        }

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStop_1_Node() throws Exception {
        startGrid(0);

        waitForTopology(1);

        stopGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStop_2_Nodes_WithCache() throws Exception {
        startGrids(2);

        for (Ignite node : G.allGrids()) {
            IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

            assertNotNull(cache);

            for (int i = 0; i < 100; i++) {
                cache.put(i, node.name());

                assertEquals(node.name(), cache.get(i));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStop_2_Nodes() throws Exception {
        startGrid(0);

        waitForTopology(1);

        startGrid(1);

        waitForTopology(2);

        for (Ignite node : G.allGrids())
            node.compute().broadcast(new DummyCallable(null));

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStop1() throws Exception {
        startGridsMultiThreaded(5, false);

        waitForTopology(5);

        stopGrid(0);

        waitForTopology(4);

        for (Ignite node : G.allGrids())
            node.compute().broadcast(new DummyCallable(null));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStop2() throws Exception {
        startGridsMultiThreaded(10, false);

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer idx) {
                stopGrid(idx);
            }
        }, 3, "stop-node-thread");

        waitForTopology(7);
    }

    /**
     * @param expSize Expected nodes number.
     * @throws Exception If failed.
     */
    private void waitForTopology(final int expSize) throws Exception {
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                List<Ignite> nodes = G.allGrids();

                if (nodes.size() != expSize) {
                    info("Wait all nodes [size=" + nodes.size() + ", exp=" + expSize + ']');

                    return false;
                }

                for (Ignite node: nodes) {
                    int sizeOnNode = node.cluster().nodes().size();

                    if (sizeOnNode != expSize) {
                        info("Wait for size on node [node=" + node.name() + ", size=" + sizeOnNode + ", exp=" + expSize + ']');

                        return false;
                    }
                }

                return true;
            }
        }, 5000));
    }

    /**
     *
     */
    private static class DummyCallable implements IgniteCallable<Object> {
        /** */
        private byte[] data;

        /**
         * @param data Data.
         */
        DummyCallable(byte[] data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return data;
        }
    }
}
