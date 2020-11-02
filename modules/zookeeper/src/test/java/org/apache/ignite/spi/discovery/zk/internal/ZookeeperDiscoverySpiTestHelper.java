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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD;

/**
 * Non-base functionality shared by some of Zookeeper SPI discovery test classes in this package.
 * 0. this rename to ...Helper and add as protected final member of base
 * 1. client and clientMode and clientThreadLoc move inside, adjust base with obtaining getters
 * 2. info into constructor as Consumer<String>
 * 3. evts into parameter of checkEvents and waitForEventsAcks
 * 4. spis into parameter of waitSpi
 */
class ZookeeperDiscoverySpiTestHelper {
    /** */
    static final String IGNITE_ZK_ROOT = ZookeeperDiscoverySpi.DFLT_ROOT_PATH;

    /** */
    private final Consumer<String> info;

    /** */
    private final AtomicInteger clusterNum;

    /** */
    ZookeeperDiscoverySpiTestHelper(Consumer<String> info, AtomicInteger clusterNum) {
        this.info = info;
        this.clusterNum = clusterNum;
    }

    /** */
    static void ackEveryEventSystemProperty() {
        System.setProperty(IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD, "1");
    }

    /**
     * @param node Node.
     * @param expEvts Expected events.
     * @throws Exception If fialed.
     */
    void checkEvents(final Ignite node, ConcurrentHashMap<UUID, Map<T2<Integer, Long>, DiscoveryEvent>> evts,
        final DiscoveryEvent...expEvts) throws Exception {
        checkEvents(node.cluster().localNode().id(), evts, expEvts);
    }

    /**
     * @param nodeId Node ID.
     * @param expEvts Expected events.
     * @throws Exception If failed.
     */
    private void checkEvents(final UUID nodeId, ConcurrentHashMap<UUID, Map<T2<Integer, Long>, DiscoveryEvent>> evts,
        final DiscoveryEvent...expEvts) throws Exception {
        Assert.assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
            @Override public boolean apply() {
                Map<T2<Integer, Long>, DiscoveryEvent> nodeEvts = evts.get(nodeId);

                if (nodeEvts == null) {
                    info.accept("No events for node: " + nodeId);

                    return false;
                }

                synchronized (nodeEvts) {
                    for (DiscoveryEvent expEvt : expEvts) {
                        DiscoveryEvent evt0 = nodeEvts.get(new T2<>(clusterNum.get(), expEvt.topologyVersion()));

                        if (evt0 == null) {
                            info.accept("No event for version: " + expEvt.topologyVersion());

                            return false;
                        }

                        Assert.assertEquals("Unexpected event [topVer=" + expEvt.topologyVersion() + //todo check
                            ", exp=" + U.gridEventName(expEvt.type()) +
                            ", evt=" + evt0 + ']', expEvt.type(), evt0.type());
                    }
                }

                return true;
            }
        }, 30000));
    }

    /**
     * @param topVer Topology version.
     * @return Expected event instance.
     */
    static DiscoveryEvent joinEvent(long topVer) {
        DiscoveryEvent expEvt = new DiscoveryEvent(null, null, EventType.EVT_NODE_JOINED, null);

        expEvt.topologySnapshot(topVer, null);

        return expEvt;
    }

    /**
     * @param topVer Topology version.
     * @return Expected event instance.
     */
    static DiscoveryEvent leftEvent(long topVer, boolean fail) {
        int eventType = fail ? EventType.EVT_NODE_FAILED : EventType.EVT_NODE_LEFT;

        DiscoveryEvent expEvt = new DiscoveryEvent(null, null, eventType, null);

        expEvt.topologySnapshot(topVer, null);

        return expEvt;
    }

    /**
     * @param nodeName Node name.
     * @return Node's discovery SPI.
     * @throws Exception If failed.
     */
    ZookeeperDiscoverySpi waitSpi(final String nodeName, ConcurrentHashMap<String, ZookeeperDiscoverySpi> spis)
        throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                ZookeeperDiscoverySpi spi = spis.get(nodeName);

                return spi != null && GridTestUtils.getFieldValue(spi, "impl") != null;

            }
        }, 5000);

        ZookeeperDiscoverySpi spi = spis.get(nodeName);

        Assert.assertNotNull("Failed to get SPI for node: " + nodeName, spi);

        return spi;
    }

    /**
     * @param spi Spi instance.
     * @return Zookeeper client.
     */
    static ZooKeeper zkClient(ZookeeperDiscoverySpi spi) {
        return GridTestUtils.getFieldValue(spi, "impl", "rtState", "zkClient", "zk");
    }

    /**
     * @param spi SPI.
     * @return Znode related to given SPI.
     */
    static String aliveZkNodePath(DiscoverySpi spi) {
        String path = GridTestUtils.getFieldValue(spi, "impl", "rtState", "locNodeZkPath");

        return path.substring(path.lastIndexOf('/') + 1);
    }

    /**
     * @param node Node.
     * @throws Exception If failed.
     */
    void waitForEventsAcks(final Ignite node) throws Exception {
        Assert.assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                Map<Object, Object> evts = GridTestUtils.getFieldValue(node.configuration().getDiscoverySpi(),
                    "impl", "rtState", "evtsData", "evts");

                if (!evts.isEmpty()) {
                    info.accept("Unacked events: " + evts);

                    return false;
                }

                return true;
            }
        }, 10_000));
    }

    /**
     * @param log Logger.
     * @param latch Latch.
     * @throws Exception If failed.
     */
    static void waitReconnectEvent(IgniteLogger log, CountDownLatch latch) throws Exception {
        if (!latch.await(30_000, MILLISECONDS)) {
            log.error("Failed to wait for reconnect event, will dump threads, latch count: " + latch.getCount());

            U.dumpThreads(log);

            Assert.fail("Failed to wait for disconnect/reconnect event.");
        }
    }

    /**
     * @param node Node.
     * @return Corresponding znode.
     */
    static String aliveZkNodePath(Ignite node) {
        return aliveZkNodePath(node.configuration().getDiscoverySpi());
    }

    /**
     * @param log Logger.
     * @param connectStr Zookeeper connect string.
     * @param failedZkNodes Znodes which should be removed.
     * @param timeout Timeout.
     * @throws Exception If failed.
     */
    static void waitNoAliveZkNodes(final IgniteLogger log,
        String connectStr,
        final List<String> failedZkNodes,
        long timeout)
        throws Exception
    {
        final ZookeeperClient zkClient = new ZookeeperClient(log, connectStr, 10_000, null);

        try {
            Assert.assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() { //todo check
                @Override public boolean apply() {
                    try {
                        List<String> c = zkClient.getChildren(IGNITE_ZK_ROOT + "/" + ZkIgnitePaths.ALIVE_NODES_DIR);

                        for (String failedZkNode : failedZkNodes) {
                            if (c.contains(failedZkNode)) {
                                log.info("Alive node is not removed [node=" + failedZkNode + ", all=" + c + ']');

                                return false;
                            }
                        }

                        return true;
                    }
                    catch (Exception e) {
                        e.printStackTrace();

                        Assert.fail();

                        return true;
                    }
                }
            }, timeout));
        }
        finally {
            zkClient.close();
        }
    }

    /** */
    static class DummyCallable implements IgniteCallable<Object> {
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
