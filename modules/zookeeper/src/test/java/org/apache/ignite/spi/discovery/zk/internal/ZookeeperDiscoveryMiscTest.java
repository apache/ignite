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

package org.apache.ignite.spi.discovery.zk.internal;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.SecurityCredentialsAttrFilterPredicate;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.lang.gridfunc.PredicateMapView;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiMBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2;

/**
 * Tests for Zookeeper SPI discovery.
 */
public class ZookeeperDiscoveryMiscTest extends ZookeeperDiscoverySpiTestBase {
    /** */
    private CacheConfiguration ccfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Verifies that node attributes returned through public API are presented in standard form.
     *
     * It means there is no exotic classes that may unnecessary capture other classes from the context.
     *
     * For more information about the problem refer to
     * <a href="https://issues.apache.org/jira/browse/IGNITE-8857">IGNITE-8857</a>.
     */
    @Test
    public void testNodeAttributesNotReferencingZookeeperClusterNode() throws Exception {
        userAttrs = new HashMap<>();
        userAttrs.put("testAttr", "testAttr");

        try {
            IgniteEx ignite = startGrid(0);

            Map<String, Object> attrs = ignite.cluster().localNode().attributes();

            assertTrue(attrs instanceof PredicateMapView);

            IgnitePredicate[] preds = GridTestUtils.getFieldValue(attrs, "preds");

            assertNotNull(preds);

            assertEquals(1, preds.length);

            assertTrue(preds[0] instanceof SecurityCredentialsAttrFilterPredicate);
        }
        finally {
            userAttrs = null;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testZkRootNotExists() throws Exception {
        zkRootPath = "/a/b/c";

        for (int i = 0; i < 3; i++) {
            reset();

            startGridsMultiThreaded(5);

            waitForTopology(5);

            stopAllGrids();

            checkEventsConsistency();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMetadataUpdate() throws Exception {
        startGrid(0);

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @SuppressWarnings("deprecation")
            @Override public Void call() throws Exception {
                ignite(0).configuration().getMarshaller().marshal(new C1());
                ignite(0).configuration().getMarshaller().marshal(new C2());

                return null;
            }
        }, 64, "marshal");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeAddresses() throws Exception {
        startGridsMultiThreaded(3);

        startClientGridsMultiThreaded(3, 3);

        waitForTopology(6);

        for (Ignite node : G.allGrids()) {
            ClusterNode locNode0 = node.cluster().localNode();

            assertTrue(!locNode0.addresses().isEmpty());
            assertTrue(!locNode0.hostNames().isEmpty());

            for (ClusterNode node0 : node.cluster().nodes()) {
                assertTrue(!node0.addresses().isEmpty());
                assertTrue(!node0.hostNames().isEmpty());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetConsistentId() throws Exception {
        startGridsMultiThreaded(3);

        startClientGridsMultiThreaded(3, 3);

        waitForTopology(6);

        for (Ignite node : G.allGrids()) {
            ClusterNode locNode0 = node.cluster().localNode();

            assertEquals(locNode0.attribute(ATTR_IGNITE_INSTANCE_NAME),
                locNode0.consistentId());

            for (ClusterNode node0 : node.cluster().nodes()) {
                assertEquals(node0.attribute(ATTR_IGNITE_INSTANCE_NAME),
                    node0.consistentId());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultConsistentId() throws Exception {
        dfltConsistenId = true;

        startGridsMultiThreaded(3);

        startClientGridsMultiThreaded(3, 3);

        waitForTopology(6);

        for (Ignite node : G.allGrids()) {
            ClusterNode locNode0 = node.cluster().localNode();

            assertNotNull(locNode0.consistentId());

            for (ClusterNode node0 : node.cluster().nodes())
                assertNotNull(node0.consistentId());
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testMbean() throws Exception {
        startGrids(3);

        UUID crdNodeId = grid(0).localNode().id();

        try {
            for (int i = 0; i < 3; i++) {
                IgniteEx grid = grid(i);

                ZookeeperDiscoverySpiMBean bean = getMxBean(grid.context().igniteInstanceName(), "SPIs",
                    ZookeeperDiscoverySpi.class, ZookeeperDiscoverySpiMBean.class);

                assertNotNull(bean);

                assertEquals(String.valueOf(grid.cluster().node(crdNodeId)), bean.getCoordinatorNodeFormatted());
                assertEquals(String.valueOf(grid.cluster().localNode()), bean.getLocalNodeFormatted());
                assertEquals(zkCluster.getConnectString(), bean.getZkConnectionString());
                assertEquals((long)grid.configuration().getFailureDetectionTimeout(), bean.getZkSessionTimeout());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientNodesStatus() throws Exception {
        startGrid(0);

        for (Ignite node : G.allGrids()) {
            assertEquals(0, node.cluster().forClients().nodes().size());
            assertEquals(1, node.cluster().forServers().nodes().size());
        }

        startClientGrid(1);

        for (Ignite node : G.allGrids()) {
            assertEquals(1, node.cluster().forClients().nodes().size());
            assertEquals(1, node.cluster().forServers().nodes().size());
        }

        startGrid(2);

        startClientGrid(3);

        for (Ignite node : G.allGrids()) {
            assertEquals(2, node.cluster().forClients().nodes().size());
            assertEquals(2, node.cluster().forServers().nodes().size());
        }

        stopGrid(1);

        waitForTopology(3);

        for (Ignite node : G.allGrids()) {
            assertEquals(1, node.cluster().forClients().nodes().size());
            assertEquals(2, node.cluster().forServers().nodes().size());
        }

        stopGrid(2);

        waitForTopology(2);

        for (Ignite node : G.allGrids()) {
            assertEquals(1, node.cluster().forClients().nodes().size());
            assertEquals(1, node.cluster().forServers().nodes().size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalAuthenticationFails() throws Exception {
        auth = ZkTestNodeAuthenticator.factory(getTestIgniteInstanceName(0));

        Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        IgniteSpiException spiErr = X.cause(err, IgniteSpiException.class);

        assertNotNull(spiErr);
        assertTrue(spiErr.getMessage().contains("Failed to authenticate local node"));

        startGrid(1);
        startGrid(2);

        checkTestSecuritySubject(2);
    }

    /**
     * Checks ZookeeperDiscovery call {@link org.apache.ignite.internal.GridComponent#validateNode(ClusterNode)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeValidation() throws Exception {
        ccfg = new CacheConfiguration("validate-test-cache");
        ccfg.setAffinity(new ValidationTestAffinity());

        startGrid(0);

        ccfg = new CacheConfiguration("validate-test-cache");
        ccfg.setAffinity(new ValidationTestAffinity());

        checkStartFail(1, "Failed to add node to topology because it has the same hash code", false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAuthentication() throws Exception {
        auth = ZkTestNodeAuthenticator.factory(getTestIgniteInstanceName(1),
            getTestIgniteInstanceName(5));

        String expErr = "Authentication failed";

        startGrid(0);

        checkTestSecuritySubject(1);

        {
            checkStartFail(1, expErr, false);
            checkStartFail(1, expErr, true);
        }

        startGrid(2);

        checkTestSecuritySubject(2);

        stopGrid(2);

        checkTestSecuritySubject(1);

        startGrid(2);

        checkTestSecuritySubject(2);

        stopGrid(0);

        checkTestSecuritySubject(1);

        checkStartFail(1, expErr, false);

        startGrid(3);

        startClientGrid(4);

        startGrid(0);

        checkTestSecuritySubject(4);

        checkStartFail(1, expErr, false);
        checkStartFail(5, expErr, false);
        checkStartFail(1, expErr, true);
        checkStartFail(5, expErr, true);
    }

    /**
     * @param nodeIdx Node index.
     * @param expMsg Expected error message.
     * @param client Client mode flag.
     */
    private void checkStartFail(final int nodeIdx, String expMsg, boolean client) {
        Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                if (client)
                    startClientGrid(nodeIdx);
                else
                    startGrid(nodeIdx);

                return null;
            }
        }, IgniteCheckedException.class, null);

        IgniteSpiException spiErr = X.cause(err, IgniteSpiException.class);

        assertNotNull(spiErr);
        assertTrue(spiErr.getMessage(), spiErr.getMessage().contains(expMsg));
    }

    /**
     * @param expNodes Expected nodes number.
     * @throws Exception If failed.
     */
    private void checkTestSecuritySubject(int expNodes) throws Exception {
        waitForTopology(expNodes);

        List<Ignite> nodes = G.allGrids();

        JdkMarshaller marsh = new JdkMarshaller();

        for (Ignite ignite : nodes) {
            Collection<ClusterNode> nodes0 = ignite.cluster().nodes();

            assertEquals(nodes.size(), nodes0.size());

            for (ClusterNode node : nodes0) {
                byte[] secSubj = node.attribute(ATTR_SECURITY_SUBJECT_V2);

                assertNotNull(secSubj);

                ZkTestNodeAuthenticator.TestSecurityContext secCtx = marsh.unmarshal(secSubj, null);

                assertEquals(node.attribute(ATTR_IGNITE_INSTANCE_NAME), secCtx.nodeName);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopNode_1() throws Exception {
        startGrids(5);

        waitForTopology(5);

        stopGrid(3);

        waitForTopology(4);

        startGrid(3);

        waitForTopology(5);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testZkMbeansValidity() throws Exception {
        try {
            Ignite ignite = startGrid();

            validateMbeans(ignite, "org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi$ZookeeperDiscoverySpiMBeanImpl");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class ValidationTestAffinity extends RendezvousAffinityFunction {
        /** {@inheritDoc} */
        @Override public Object resolveNodeHash(ClusterNode node) {
            return 0;
        }
    }

    /** */
    private static class C1 implements Serializable {
        // No-op.
    }

    /** */
    private static class C2 implements Serializable {
        // No-op.
    }

    /** */
    static class ZkTestNodeAuthenticator implements DiscoverySpiNodeAuthenticator {
        /**
         * @param failAuthNodes Node names which should not pass authentication.
         * @return Factory.
         */
        static IgniteOutClosure<DiscoverySpiNodeAuthenticator> factory(final String...failAuthNodes) {
            return new IgniteOutClosure<DiscoverySpiNodeAuthenticator>() {
                @Override public DiscoverySpiNodeAuthenticator apply() {
                    return new ZkTestNodeAuthenticator(Arrays.asList(failAuthNodes));
                }
            };
        }

        /** */
        private final Collection<String> failAuthNodes;

        /**
         * @param failAuthNodes Node names which should not pass authentication.
         */
        ZkTestNodeAuthenticator(Collection<String> failAuthNodes) {
            this.failAuthNodes = failAuthNodes;
        }

        /** {@inheritDoc} */
        @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
            assertNotNull(cred);

            String nodeName = node.attribute(ATTR_IGNITE_INSTANCE_NAME);

            assertEquals(nodeName, cred.getUserObject());

            boolean auth = !failAuthNodes.contains(nodeName);

            System.out.println(Thread.currentThread().getName() + " authenticateNode [node=" + node.id() + ", res=" + auth + ']');

            return auth ? new TestSecurityContext(nodeName) : null;
        }

        /** {@inheritDoc} */
        @Override public boolean isGlobalNodeAuthentication() {
            return false;
        }

        /**
         *
         */
        private static class TestSecurityContext implements SecurityContext, Serializable {
            /** Serial version uid. */
            private static final long serialVersionUID = 0L;

            /** */
            final String nodeName;

            /**
             * @param nodeName Authenticated node name.
             */
            TestSecurityContext(String nodeName) {
                this.nodeName = nodeName;
            }

            /** {@inheritDoc} */
            @Override public SecuritySubject subject() {
                return null;
            }

            /** {@inheritDoc} */
            @Override public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
                return true;
            }

            /** {@inheritDoc} */
            @Override public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
                return true;
            }

            /** {@inheritDoc} */
            @Override public boolean serviceOperationAllowed(String srvcName, SecurityPermission perm) {
                return true;
            }

            /** {@inheritDoc} */
            @Override public boolean systemOperationAllowed(SecurityPermission perm) {
                return true;
            }
        }
    }
}
