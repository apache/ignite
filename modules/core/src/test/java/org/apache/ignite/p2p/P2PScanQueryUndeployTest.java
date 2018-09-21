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
package org.apache.ignite.p2p;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class P2PScanQueryUndeployTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_PREDICATE_RESOURCE_NAME = TestPredicate.class.getSimpleName() + ".class";
    /** Cache name. */
    private static final String CACHE_NAME = "test-cache";

    /** Client instance name. */
    private static final String CLIENT_INSTANCE_NAME = "client";

    /** */
    private String propValBeforeTest;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(true);

        cfg.setCacheConfiguration(
            new CacheConfiguration()
                .setName(CACHE_NAME)
                .setBackups(1)
        );

        cfg.setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(
                    new TcpDiscoveryVmIpFinder(true)
                        .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                )
        );

        cfg.setPeerClassLoadingLocalClassPathExclude(TestPredicate.class.getName());

        cfg.setCommunicationSpi(new MessageCountingCommunicationSpi());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration());

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        if (igniteInstanceName.equals(CLIENT_INSTANCE_NAME))
            cfg.setClientMode(true);

        return cfg;
    }

    @Override protected void checkConfiguration(IgniteConfiguration cfg) {
        // No op.
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        return super.isRemoteJvm(igniteInstanceName) && !igniteInstanceName.equals(CLIENT_INSTANCE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Checks, that after client's reconnect to cluster will be redeployed from client node.
     *
     * @throws Exception if test failed.
     */
    public void testAfterClientDisconnect() throws Exception {
        // Starting server node on remote JVM and client node on local JVM.
        startGrids(2);

        Ignite client = startGrid(CLIENT_INSTANCE_NAME);

        stopGrid(0);

        while (client.cluster().topologyVersion() != 4)
            U.sleep(10L);

        client.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, String> cache = client.getOrCreateCache(CACHE_NAME);

        cache.put(1, "foo");

        int gridDeploymentRequests = client
            .compute(client.cluster().forRemotes())
            .call(MessageCountingCommunicationSpi::deploymentRequestCount);

        assertEquals("Invalid number of sent grid deployment requests", 0, gridDeploymentRequests);

        cache.query(new ScanQuery<>(new TestPredicate())).getAll();

        gridDeploymentRequests = client
            .compute(client.cluster().forRemotes())
            .call(MessageCountingCommunicationSpi::deploymentRequestCount);

        assertEquals("Invalid number of sent grid deployment requests", 1, gridDeploymentRequests);

        stopGrid(CLIENT_INSTANCE_NAME);

        client = startGrid(CLIENT_INSTANCE_NAME);

        cache = client.getOrCreateCache(CACHE_NAME);

        cache.query(new ScanQuery<>(new TestPredicate())).getAll();

        gridDeploymentRequests = client
            .compute(client.cluster().forRemotes())
            .call(MessageCountingCommunicationSpi::deploymentRequestCount);

        assertEquals("Invalid number of sent grid deployment requests", 2, gridDeploymentRequests);
    }

    /** */
    private static class MessageCountingCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private static final AtomicInteger reqCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackClosure
        ) throws IgniteSpiException {
            if (msg instanceof GridIoMessage && isDeploymentRequestMessage((GridIoMessage)msg))
                reqCnt.incrementAndGet();

            super.sendMessage(node, msg, ackClosure);
        }

        /**
         * @return Number of deployment requests.
         */
        public static int deploymentRequestCount() {
            return reqCnt.get();
        }

        /**
         * Checks if it is a p2p deployment request message with test predicate resource.
         *
         * @param msg Message to check.
         * @return {@code True} if this is a p2p message.
         */
        private boolean isDeploymentRequestMessage(GridIoMessage msg) {
            try {
                if (msg.message() instanceof GridDeploymentRequest) {
                    // rsrcName getter have only default access level. So, use reflection for access to field value.
                    GridDeploymentRequest req = (GridDeploymentRequest)msg.message();

                    Field f = GridDeploymentRequest.class.getDeclaredField("rsrcName");

                    f.setAccessible(true);

                    return ((String)f.get(req)).endsWith(TEST_PREDICATE_RESOURCE_NAME);
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            return false;
        }
    }

    /** */
    private static class TestPredicate implements IgniteBiPredicate<Integer, String> {
        /** {@inheritDoc} */
        @Override public boolean apply(Integer integer, String s) {
            return true;
        }
    }

}