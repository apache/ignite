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

package org.apache.ignite.spi.discovery.tcp.ipfinder.zk;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.utils.CloseableUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for {@link TcpDiscoveryZookeeperIpFinder}.
 *
 * @author Raul Kripalani
 */
public class ZookeeperIpFinderTest extends GridCommonAbstractTest {
    /** ZK Cluster size. */
    private static final int ZK_CLUSTER_SIZE = 3;

    /** ZK Path size. */
    private static final String SERVICES_IGNITE_ZK_PATH = "/services/ignite";

    /** The ZK cluster instance, from curator-test. */
    private TestingCluster zkCluster;

    /** A Curator client to perform assertions on the embedded ZK instances. */
    private CuratorFramework zkCurator;

    /** Whether to allow duplicate registrations for the current test method or not. */
    private boolean allowDuplicateRegistrations = false;

    /** Constructor that does not start any grids. */
    public ZookeeperIpFinderTest() {
        super(false);
    }

    /**
     * Before test.
     *
     * @throws Exception
     */
    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        // remove stale system properties
        System.getProperties().remove(TcpDiscoveryZookeeperIpFinder.PROP_ZK_CONNECTION_STRING);

        // disable JMX for tests
        System.setProperty("zookeeper.jmx.log4j.disable", "true");

        // start the ZK cluster
        zkCluster = new TestingCluster(ZK_CLUSTER_SIZE);
        zkCluster.start();

        // start the Curator client so we can perform assertions on the ZK state later
        zkCurator = CuratorFrameworkFactory.newClient(zkCluster.getConnectString(), new RetryNTimes(10, 1000));
        zkCurator.start();
    }

    /**
     * After test.
     *
     * @throws Exception
     */
    @Override public void afterTest() throws Exception {
        super.afterTest();

        if (zkCurator != null)
            CloseableUtils.closeQuietly(zkCurator);

        if (zkCluster != null)
            CloseableUtils.closeQuietly(zkCluster);

        stopAllGrids();
    }

    /**
     * Enhances the default configuration with the {#TcpDiscoveryZookeeperIpFinder}.
     *
     * @param gridName Grid name.
     * @return Ignite configuration.
     * @throws Exception If failed.
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(gridName);

        TcpDiscoverySpi tcpDisco = (TcpDiscoverySpi)configuration.getDiscoverySpi();
        TcpDiscoveryZookeeperIpFinder zkIpFinder = new TcpDiscoveryZookeeperIpFinder();
        zkIpFinder.setAllowDuplicateRegistrations(allowDuplicateRegistrations);

        // first node => configure with zkUrl; second node => configure with CuratorFramework; third and subsequent
        // shall be configured through system property
        if (gridName.equals(getTestGridName(0)))
            zkIpFinder.setZkConnectionString(zkCluster.getConnectString());

        else if (gridName.equals(getTestGridName(1))) {
            zkIpFinder.setCurator(CuratorFrameworkFactory.newClient(zkCluster.getConnectString(),
                new ExponentialBackoffRetry(100, 5)));
        }

        tcpDisco.setIpFinder(zkIpFinder);

        return configuration;
    }

    /**
     * @throws Exception If failed.
     */
    public void testOneIgniteNodeIsAlone() throws Exception {
        startGrid(0);

        assertEquals(1, grid(0).cluster().metrics().getTotalNodes());

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoIgniteNodesFindEachOther() throws Exception {
        // start one node
        startGrid(0);

        // set up an event listener to expect one NODE_JOINED event
        CountDownLatch latch = expectJoinEvents(grid(0), 1);

        // start the other node
        startGrid(1);

        // assert the nodes see each other
        assertEquals(2, grid(0).cluster().metrics().getTotalNodes());
        assertEquals(2, grid(1).cluster().metrics().getTotalNodes());

        // assert the event listener got as many events as expected
        latch.await(1, TimeUnit.SECONDS);

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testThreeNodesWithThreeDifferentConfigMethods() throws Exception {
        // start one node
        startGrid(0);

        // set up an event listener to expect one NODE_JOINED event
        CountDownLatch latch = expectJoinEvents(grid(0), 2);

        // start the 2nd node
        startGrid(1);

        // start the 3rd node, first setting the system property
        System.setProperty(TcpDiscoveryZookeeperIpFinder.PROP_ZK_CONNECTION_STRING, zkCluster.getConnectString());
        startGrid(2);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 2);

        // assert the nodes see each other
        assertEquals(3, grid(0).cluster().metrics().getTotalNodes());
        assertEquals(3, grid(1).cluster().metrics().getTotalNodes());
        assertEquals(3, grid(2).cluster().metrics().getTotalNodes());

        // assert the event listener got as many events as expected
        latch.await(1, TimeUnit.SECONDS);

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFourNodesStartingAndStopping() throws Exception {
        // start one node
        startGrid(0);

        // set up an event listener to expect one NODE_JOINED event
        CountDownLatch latch = expectJoinEvents(grid(0), 3);

        // start the 2nd node
        startGrid(1);

        // start the 3rd & 4th nodes, first setting the system property
        System.setProperty(TcpDiscoveryZookeeperIpFinder.PROP_ZK_CONNECTION_STRING, zkCluster.getConnectString());
        startGrid(2);
        startGrid(3);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 3);

        // assert the nodes see each other
        assertEquals(4, grid(0).cluster().metrics().getTotalNodes());
        assertEquals(4, grid(1).cluster().metrics().getTotalNodes());
        assertEquals(4, grid(2).cluster().metrics().getTotalNodes());
        assertEquals(4, grid(3).cluster().metrics().getTotalNodes());

        // assert the event listener got as many events as expected
        latch.await(1, TimeUnit.SECONDS);

        // stop the first grid
        stopGrid(0);

        // make sure that nodes were synchronized; they should only see 3 now
        assertEquals(3, grid(1).cluster().metrics().getTotalNodes());
        assertEquals(3, grid(2).cluster().metrics().getTotalNodes());
        assertEquals(3, grid(3).cluster().metrics().getTotalNodes());

        // stop all remaining grids
        stopGrid(1);
        stopGrid(2);
        stopGrid(3);

        // check that the nodes are gone in ZK
        assertEquals(0, zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFourNodesWithDuplicateRegistrations() throws Exception {
        allowDuplicateRegistrations = true;

        // start 4 nodes
        System.setProperty(TcpDiscoveryZookeeperIpFinder.PROP_ZK_CONNECTION_STRING, zkCluster.getConnectString());
        startGrids(4);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 3);

        // each node will register itself + the node that it connected to to join the cluster
        assertEquals(7, zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size());

        // stop all grids
        stopAllGrids();

        // check that all nodes are gone in ZK
        assertEquals(0, zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFourNodesWithNoDuplicateRegistrations() throws Exception {
        allowDuplicateRegistrations = false;

        // start 4 nodes
        System.setProperty(TcpDiscoveryZookeeperIpFinder.PROP_ZK_CONNECTION_STRING, zkCluster.getConnectString());
        startGrids(4);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 3);

        // each node will only register itself
        assertEquals(4, zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size());

        // stop all grids
        stopAllGrids();

        // check that all nodes are gone in ZK
        assertEquals(0, zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFourNodesRestartLastSeveralTimes() throws Exception {
        allowDuplicateRegistrations = false;

        // start 4 nodes
        System.setProperty(TcpDiscoveryZookeeperIpFinder.PROP_ZK_CONNECTION_STRING, zkCluster.getConnectString());
        startGrids(4);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 3);

        // each node will only register itself
        assertEquals(4, zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size());

        // repeat 5 times
        for (int i = 0; i < 5; i++) {
            // stop last grid
            stopGrid(2);

            // check that the node has unregistered itself and its party
            assertEquals(3, zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size());

            // start the node again
            startGrid(2);

            // check that the node back in ZK
            assertEquals(4, zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size());
        }

        stopAllGrids();

        assertEquals(0, zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFourNodesKillRestartZookeeper() throws Exception {
        allowDuplicateRegistrations = false;

        // start 4 nodes
        System.setProperty(TcpDiscoveryZookeeperIpFinder.PROP_ZK_CONNECTION_STRING, zkCluster.getConnectString());
        startGrids(4);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 3);

        // each node will only register itself
        assertEquals(4, zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size());

        // remember ZK server configuration and stop the cluster
        Collection<InstanceSpec> instances = zkCluster.getInstances();
        zkCluster.stop();
        Thread.sleep(1000);

        // start the cluster with the previous configuration
        zkCluster = new TestingCluster(instances);
        zkCluster.start();

        // block the client until connected
        zkCurator.blockUntilConnected();

        // check that the nodes have registered again
        assertEquals(4, zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size());

        // stop all grids
        stopAllGrids();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    return 0 == zkCurator.getChildren().forPath(SERVICES_IGNITE_ZK_PATH).size();
                }
                catch (Exception ignored) {
                    return false;
                }
            }
        }, 20000));
    }

    /**
     * @param ignite Node.
     * @param joinEvtCnt Expected events number.
     * @return Events latch.
     */
    private CountDownLatch expectJoinEvents(Ignite ignite, int joinEvtCnt) {
        final CountDownLatch latch = new CountDownLatch(joinEvtCnt);

        ignite.events().remoteListen(new IgniteBiPredicate<UUID, Event>() {
            @Override public boolean apply(UUID uuid, Event evt) {
                latch.countDown();
                return true;
            }
        }, null, EventType.EVT_NODE_JOINED);

        return latch;
    }
}
