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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryForever;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.managers.discovery.JoiningNodesAware;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.DiscoverySpiHistorySupport;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.DiscoverySpiOrderSupport;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@IgniteSpiMultipleInstancesSupport(true)
@DiscoverySpiOrderSupport(true)
@DiscoverySpiHistorySupport(true)
public class ZookeeperDiscoverySpi extends IgniteSpiAdapter implements DiscoverySpi, JoiningNodesAware {
    /** */
    private static final String IGNITE_PATH = "/ignite";

    /** */
    private static final String IGNITE_INIT_LOCK_PATH = "/igniteLock";

    /** */
    private static final String CLUSTER_PATH = IGNITE_PATH + "/cluster";

    /** */
    private static final String EVENTS_PATH = CLUSTER_PATH + "/events";

    /** */
    private static final String JOIN_HIST_PATH = CLUSTER_PATH + "/joinHist";

    /** */
    private static final String ALIVE_NODES_PATH = CLUSTER_PATH + "/alive";

    /** */
    private static final String CUSTOM_EVTS_PATH = CLUSTER_PATH + "/customEvts";

    /** */
    private static final String DISCO_EVTS_HIST_PATH = CLUSTER_PATH + "/evtsHist";

    /** */
    private static final byte[] EMPTY_BYTES = new byte[0];

    /** */
    private String connectString;

    /** */
    private DiscoverySpiListener lsnr;

    /** */
    private DiscoverySpiDataExchange exchange;

    /** */
    private DiscoveryMetricsProvider metricsProvider;

    /** */
    private ZooKeeper zk;

    /** */
    private CuratorFramework zkCurator;

    /** */
    private int sesTimeout = 5000;

    /** */
    private final ZookeeperWatcher zkWatcher;

    /** */
    private final JdkMarshaller marsh = new JdkMarshaller();

    /** */
    private final ZKChildrenUpdateCallback zkChildrenUpdateCallback;

    /** */
    private final DataUpdateCallback dataUpdateCallback;

    /** */
    private final JoinedNodes joinHist = new JoinedNodes();

    /** */
    private ZookeeperClusterNode locNode;

    /** */
    private Map<String, Object> locNodeAttrs;

    /** */
    private IgniteProductVersion locNodeVer;

    /** */
    private long gridStartTime;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** */
    private CountDownLatch joinLatch = new CountDownLatch(1);

    /** */
    private Exception joinErr;

    /** For testing only. */
    private CountDownLatch connectStart = new CountDownLatch(1);

    /**
     *
     */
    public ZookeeperDiscoverySpi() {
        zkWatcher = new ZookeeperWatcher();

        zkChildrenUpdateCallback = new ZKChildrenUpdateCallback();
        dataUpdateCallback = new DataUpdateCallback();
    }

    public int getSessionTimeout() {
        return sesTimeout;
    }

    public void setSessionTimeout(int sesTimeout) {
        this.sesTimeout = sesTimeout;
    }

    public String getConnectString() {
        return connectString;
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    /** */
    private Serializable consistentId;

    /** {@inheritDoc} */
    @Override public boolean knownNode(UUID nodeId) {
        try {
            for (String child : zkCurator.getChildren().forPath(ALIVE_NODES_PATH)) {
                ZKNodeData nodeData = parseNodePath(child);

                if (nodeData.nodeId.equals(nodeId))
                    return true;
            }

            return false;
        }
        catch (Exception e) {
            U.error(log, "Failed to read alive nodes: " + e, e);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable consistentId() throws IgniteSpiException {
        if (consistentId == null) {
            final Serializable cfgId = ignite.configuration().getConsistentId();

            consistentId = cfgId;
        }

        return consistentId;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> getRemoteNodes() {
        // TODO ZK
        List<ClusterNode> nodes;

        synchronized (curTop) {
            nodes = new ArrayList<>((Collection)curTop.values());

            for (ClusterNode node : curTop.values()) {
                if (!locNode.id().equals(node.id()))
                    nodes.add(node);
            }
        }

        return nodes;
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getLocalNode() {
        return locNode;
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode getNode(UUID nodeId) {
        // TODO ZK 
        synchronized (curTop) {
            for (ClusterNode node : curTop.values()) {
                if (node.id().equals(nodeId))
                    return node;
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        // TODO ZK
        return getNode(nodeId) != null;
    }

    /** {@inheritDoc} */
    @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
        assert locNodeAttrs == null;
        assert locNodeVer == null;

        if (log.isDebugEnabled()) {
            log.debug("Node attributes to set: " + attrs);
            log.debug("Node version to set: " + ver);
        }

        locNodeAttrs = attrs;
        locNodeVer = ver;
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public void setDataExchange(DiscoverySpiDataExchange exchange) {
        this.exchange = exchange;
    }

    /** {@inheritDoc} */
    @Override public void setMetricsProvider(DiscoveryMetricsProvider metricsProvider) {
        this.metricsProvider = metricsProvider;
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws IgniteSpiException {
        // TODO ZK
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(DiscoverySpiNodeAuthenticator auth) {
        // TODO ZK
    }

    /** {@inheritDoc} */
    @Override public long getGridStartTime() {
        return gridStartTime;
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
        // TODO ZK
        try {
            zkCurator.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(CUSTOM_EVTS_PATH, marshal(msg));
        }
        catch (Exception e) {
            throw new IgniteSpiException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId, @Nullable String warning) {
        // TODO ZK
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isClientMode() throws IllegalStateException {
        return locNode.isClient();
    }

    /**
     *
     */
    private void initLocalNode() {
        assert ignite != null;

        Serializable consistentId = consistentId();

        UUID nodeId = ignite.configuration().getNodeId();

        // TODO ZK
        if (consistentId == null)
            consistentId = nodeId;

        locNode = new ZookeeperClusterNode(nodeId,
            locNodeVer,
            locNodeAttrs,
            consistentId,
            ignite.configuration().isClientMode());

        locNode.local(true);

        DiscoverySpiListener lsnr = this.lsnr;

        if (lsnr != null)
            lsnr.onLocalNodeInitialized(locNode);

        if (log.isDebugEnabled())
            log.debug("Local node initialized: " + locNode);
    }

    private boolean igniteClusterStarted() throws Exception {
        return zkCurator.checkExists().forPath(IGNITE_PATH) != null &&
            zkCurator.checkExists().forPath(ALIVE_NODES_PATH) != null &&
            !zk.getChildren(ALIVE_NODES_PATH, false).isEmpty();
    }

    private void startConnect(DiscoveryDataBag discoDataBag) throws Exception {
        ZKClusterData clusterData = unmarshal(zk.getData(CLUSTER_PATH, false, null));

        gridStartTime = clusterData.gridStartTime;

        zk.getData(EVENTS_PATH, zkWatcher, dataUpdateCallback, null);
        //zk.getChildren(JOIN_HIST_PATH, zkWatcher, zkChildrenUpdateCallback, null);
        zk.getChildren(ALIVE_NODES_PATH, zkWatcher, zkChildrenUpdateCallback, null);

        ZKJoiningNodeData joinData = new ZKJoiningNodeData(locNode, discoDataBag.joiningNodeData());

        byte[] nodeData = marshal(joinData);

        String zkNode = "/" + locNode.id().toString() + "-";

        zkCurator.inTransaction().
                create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(JOIN_HIST_PATH + zkNode, nodeData).
                and().
                create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(ALIVE_NODES_PATH + zkNode).
                and().commit();

        connectStart.countDown();
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        try {
            initLocalNode();

            DiscoveryDataBag discoDataBag = new DiscoveryDataBag(locNode.id());

            exchange.collect(discoDataBag);

            String threadName = Thread.currentThread().getName();

            // ZK generates internal threads' names using current thread name.
            Thread.currentThread().setName("zk-" + igniteInstanceName);

            try {
                zkCurator = CuratorFrameworkFactory.newClient(connectString, sesTimeout, sesTimeout, new RetryForever(500));

                zkCurator.getCuratorListenable().addListener(new CuratorListener() {
                    @Override public void eventReceived(CuratorFramework client, CuratorEvent evt) throws Exception {
                        IgniteLogger log = ZookeeperDiscoverySpi.this.log;

                        if (log != null)
                            log.info("Curator event: " + evt.getType());
                    }
                });
                zkCurator.getConnectionStateListenable().addListener(new ConnectionStateListener() {
                    @Override public void stateChanged(CuratorFramework client, ConnectionState newState) {
                        IgniteLogger log = ZookeeperDiscoverySpi.this.log;

                        if (log != null)
                            log.info("Curator event, connection: " + newState);

                        if (newState == ConnectionState.LOST) {
                            U.warn(log, "Connection was lost, local node SEGMENTED");

                            zkCurator.close();

                            lsnr.onDiscovery(EventType.EVT_NODE_SEGMENTED,
                                0,
                                locNode,
                                Collections.<ClusterNode>emptyList(),
                                Collections.<Long, Collection<ClusterNode>>emptyMap(),
                                null);
                        }
                    }
                });

                zkCurator.start();

                zk = zkCurator.getZookeeperClient().getZooKeeper();
                // zk = new ZooKeeper(connectString, sesTimeout, zkWatcher);
            }
            finally {
                Thread.currentThread().setName(threadName);
            }

            boolean startedConnect = false;

            for (;;) {
                boolean started = igniteClusterStarted();

                if (!started) {
                    InterProcessMutex mux = new InterProcessMutex(zkCurator, IGNITE_INIT_LOCK_PATH);

                    mux.acquire();

                    try {
                        started = igniteClusterStarted();

                        if (!started) {
                            log.info("First node starts, reset ZK state");

                            if (zkCurator.checkExists().forPath(IGNITE_PATH) != null)
                                zkCurator.delete().deletingChildrenIfNeeded().forPath(IGNITE_PATH);

                            // TODO ZK: properly handle first node start and init after full cluster restart.
                            if (zkCurator.checkExists().forPath(IGNITE_PATH) == null) {
                                log.info("Initialize Zookeeper nodes.");

                                ZKClusterData clusterData = new ZKClusterData(U.currentTimeMillis());

                                zkCurator.inTransaction().
                                    create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(IGNITE_PATH, EMPTY_BYTES).
                                    and().
                                    create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(CLUSTER_PATH, marshal(clusterData)).
                                    and().
                                    create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(JOIN_HIST_PATH, EMPTY_BYTES).
                                    and().
                                    create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(ALIVE_NODES_PATH, EMPTY_BYTES).
                                    and().
                                    create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(EVENTS_PATH, EMPTY_BYTES).
                                    and().
                                    create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(DISCO_EVTS_HIST_PATH, EMPTY_BYTES).
                                    and().
                                    create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(CUSTOM_EVTS_PATH, EMPTY_BYTES).
                                    and().commit();

                                startConnect(discoDataBag);

                                startedConnect = true;
                            }

                            break;
                        }
                    }
                    finally {
                        mux.release();
                    }
                }
                else
                    break;
            }

            if (!startedConnect)
                startConnect(discoDataBag);

            log.info("Waiting for local join event [nodeId=" + locNode.id() + ", name=" + igniteInstanceName + ']');

            for(;;) {
                if (!joinLatch.await(10, TimeUnit.SECONDS)) {
                    U.warn(log, "Waiting for local join event [nodeId=" + locNode.id() + ", name=" + igniteInstanceName + ']');
                }
                else
                    break;
            }

            if (joinErr != null)
                throw new IgniteSpiException(joinErr);
        }
        catch (Exception e) {
            connectStart.countDown();

            throw new IgniteSpiException(e);
        }
    }

    /**
     * For testing only.
     *
     * @throws Exception If failed.
     */
    public void waitConnectStart() throws Exception {
        connectStart.await();
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        closeZkClient();
    }

    private void closeZkClient() {
        if (zk != null) {
            try {
                log.info("Close Zookeeper client.");

                zk.close();
            }
            catch (Exception e) {
                U.warn(log, "Failed to stop zookeeper client: " + e, e);
            }

            zk = null;
        }
    }

    private <T> T unmarshal(byte[] data) {
        try {
            return marsh.unmarshal(data, null);
        }
        catch (Exception e) {
            U.error(log, "Unmarshal error: " + e);

            throw new IgniteException(e);
        }
    }

    private byte[] marshal(Object obj) {
        try {
            return marsh.marshal(obj);
        }
        catch (Exception e) {
            U.error(log, "Marshal error: " + e);

            throw new IgniteException(e);
        }
    }

    /** */
    private static final int ID_LEN = 36;

    /**
     *
     */
    static class ZKNodeData implements Serializable {
        /** */
        @GridToStringInclude
        final long order;

        /** */
        @GridToStringInclude
        final UUID nodeId;

        /** */
        transient ZKJoiningNodeData joinData;

        /**
         * @param order Node order.
         * @param nodeId Node ID.
         */
        ZKNodeData(long order, UUID nodeId) {
            this.order = order;
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ZKNodeData.class, this);
        }
    }

    /**
     *
     */
    static class ZKAliveNodes implements Serializable {
        /** */
        @GridToStringInclude
        final int ver;

        /** */
        @GridToStringInclude
        final TreeMap<Long, ZKNodeData> nodesByOrder;

        /**
         * @param ver
         * @param nodesByOrder
         */
        ZKAliveNodes(int ver, TreeMap<Long, ZKNodeData> nodesByOrder) {
            this.ver = ver;
            this.nodesByOrder = nodesByOrder;
        }

        ZKNodeData nodeById(UUID nodeId) {
            for (ZKNodeData nodeData : nodesByOrder.values()) {
                if (nodeId.equals(nodeData.nodeId))
                    return nodeData;
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ZKAliveNodes.class, this);
        }
    }

    /**
     * @param path Zookeeper node path.
     * @return Ignite node data.
     */
    private static ZKNodeData parseNodePath(String path) {
        String idStr = path.substring(0, ID_LEN);

        UUID nodeId = UUID.fromString(idStr);

        int nodeOrder = Integer.parseInt(path.substring(ID_LEN + 1)) + 1;

        return new ZKNodeData(nodeOrder, nodeId);
    }

    /** */
    private boolean crd;

    /** */
    private ZKAliveNodes curAlive;

    private void readJoinNodeData(ZKNodeData data, String path) throws Exception {
        //byte[] bytes = zk.getData(path, null, null);
        byte[] bytes = zkCurator.getData().forPath(path);

        assert bytes.length > 0;

        ZKJoiningNodeData joinData = unmarshal(bytes);

        assert joinData != null && joinData.node != null && joinData.joiningNodeData != null : joinData;

        joinData.node.internalOrder(data.order);

        data.joinData = joinData;
    }

    private void processJoinedNodesHistory(List<String> children, long joinOrder) {
        for (String child : children) {
            ZKNodeData data = parseNodePath(child);

            if (data.order >= joinOrder && !joinHist.hist.containsKey(data.order)) {
                try {
                    Object old = joinHist.hist.put(data.order, data);

                    assert old == null : old;

                    readJoinNodeData(data, JOIN_HIST_PATH + "/" + child);

                    assert data.joinData != null && joinHist.hist.get(data.order) == data : data;

                    log.info("New joined node data: " + data);
                }
                catch (Exception e) {
                    // TODO ZK
                    U.error(log, "Failed to get node data: " + e, e);
                }
            }
        }
    }

    /**
     *
     */
    private static class JoinedNodes {
        /** */
        private Stat stat;

        /** */
        private final Map<Long, ZKNodeData> hist = new HashMap<>();
    }

    /**
     *
     */
    class ZKChildrenUpdateCallback implements AsyncCallback.Children2Callback {
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            try {
                if (children == null || children.isEmpty())
                    return;

                if (path.equals(JOIN_HIST_PATH)) {
                    log.info("Join nodes changed [rc=" + rc +
                        ", path=" + path +
                        ", nodes=" + children +
                        ", ver=" + (stat != null ? stat.getCversion() : null) + ']');
//
//                    if (stat != null)
//                        joinHist.stat = stat;
//
//                    processJoinedNodesHistory(children);
                }
                else if (path.equals(ALIVE_NODES_PATH)) {
                    log.info("Alive nodes changed [rc=" + rc +
                        ", path=" + path +
                        ", nodes=" + children +
                        ", ver=" + (stat != null ? stat.getCversion() : null) + ']');

                    assert stat != null;

                    TreeMap<Long, ZKNodeData> nodes = new TreeMap<>();

                    for (String child : children) {
                        ZKNodeData data = parseNodePath(child);

                        nodes.put(data.order, data);
                    }

                    ZKAliveNodes newAlive = new ZKAliveNodes(stat.getCversion(), nodes);

                    generateEvents(curAlive, newAlive);

                    curAlive = newAlive;
                }
            }
            catch (Throwable e) {
                log.info("Uncaught error: " + e);

                throw e;
            }
        }
    }

    /**
     * For testing only.
     */
    public void closeClient() {
        closeZkClient();

        joinErr = new Exception("Start error");

        joinLatch.countDown();
    }

    /** */
    private final TreeMap<Long, ZookeeperClusterNode> curTop = new TreeMap<>();

    /**
     * @param oldNodes Previous processed state.
     * @param newNodes Current state.
     */
    private void generateEvents(ZKAliveNodes oldNodes, ZKAliveNodes newNodes) {
        assert newNodes != null;

        ZKNodeData locNode = newNodes.nodeById(this.locNode.id());

        if (locNode == null)
            return;

        if (!crd && newNodes.nodesByOrder.firstKey() == locNode.order) {
            log.info("Node become coordinator [oldNodes=" + oldNodes + ", curEvts=" + curEvts + ']');

            if (curEvts != null) {
                assert curEvts.aliveNodes != null;

                oldNodes = curEvts.aliveNodes;

                log.info("Node coordinator use old nodes from last events [oldNodes=" + oldNodes + ']');
            }
            else if (oldNodes == null) {
                oldNodes = new ZKAliveNodes(0, new TreeMap<Long, ZKNodeData>());

                log.info("Node coordinator init old nodes [oldNodes=" + oldNodes + ']');
            }

            curCrdEvts = curEvts;

            crd = true;
        }

        if (!crd)
            return;

        long nextJoinOrder = curCrdEvts != null ? curCrdEvts.nextJoinOrder : 1L;

        log.info("Generate discovery events [oldNodes=" + oldNodes +
            ", newNodes=" + newNodes +
            ", nextJoinOrder=" + nextJoinOrder + ']');

        if (oldNodes.ver == newNodes.ver)
            return;

        TreeMap<Integer, ZKDiscoveryEvent> evts = new TreeMap<>();

        Set<Long> failedNodes = new HashSet<>();

        for (int v = oldNodes.ver + 1; v <= newNodes.ver; v++) {
            ZKNodeData joined = joinHist.hist.get(nextJoinOrder);

            if (joined == null) {
                try {
                    // TODO ZK: check version.
                    List<String> children = zkCurator.getChildren().forPath(JOIN_HIST_PATH);

                    processJoinedNodesHistory(children, nextJoinOrder);

                    joined = joinHist.hist.get(nextJoinOrder);
                }
                catch (Exception e) {
                    U.error(log, "Failed to read joined nodes: " + e, e);
                }
            }

            // TODO ZK: first send, then process?

            if (joined != null) {
                assert joined.joinData != null : joined;

                ZKJoiningNodeData joinData = joined.joinData;

                synchronized (curTop) {
                    joinData.node.order(v);

                    curTop.put(joinData.node.internalOrder(), joinData.node);

                    ZKDiscoveryEvent joinEvt = new ZKDiscoveryEvent(EventType.EVT_NODE_JOINED,
                        v,
                        joinData.node,
                        new ArrayList<>(curTop.values()));

                    log.info("ZK event [type=JOIN, node=" + joinData.node.id() + ", ver=" + v + ']');

                    if (!joinData.node.id().equals(locNode.nodeId)) {
                        DiscoveryDataBag joiningNodeBag = new DiscoveryDataBag(joinData.node.id());

                        joiningNodeBag.joiningNodeData(joinData.joiningNodeData);

                        exchange.onExchange(joiningNodeBag);

                        DiscoveryDataBag collectBag = new DiscoveryDataBag(joinData.node.id(), new HashSet<Integer>());

                        exchange.collect(collectBag);

                        joinEvt.discoveryData(joinData.joiningNodeData, collectBag.commonData());
                    }

                    evts.put(v, joinEvt);

                    if (!newNodes.nodesByOrder.containsKey(joined.order)) {
                        v++;

                        ZookeeperClusterNode failedNode = curTop.remove(joined.order);

                        assert failedNode != null : joined.order;

                        log.info("ZK event [type=FAIL, node=" + failedNode.id() + ", ver=" + v + ']');

                        evts.put(v, new ZKDiscoveryEvent(EventType.EVT_NODE_FAILED,
                            v,
                            failedNode,
                            new ArrayList<>(curTop.values())));

                        joinHist.hist.remove(joined.order);
                    }

                    nextJoinOrder++;
                }
            }
            else {
                for (ZKNodeData oldData : oldNodes.nodesByOrder.values()) {
                    if (!failedNodes.contains(oldData.order) && !newNodes.nodesByOrder.containsKey(oldData.order)) {
                        failedNodes.add(oldData.order);

                        synchronized (curTop) {
                            ZookeeperClusterNode failedNode = curTop.remove(oldData.order);

                            assert failedNode != null : oldData.order;

                            log.info("ZK event [type=FAIL, node=" + failedNode.id() + ", ver=" + v + ']');

                            evts.put(v, new ZKDiscoveryEvent(EventType.EVT_NODE_FAILED,
                                v,
                                failedNode,
                                new ArrayList<>(curTop.values())));

                            joinHist.hist.remove(oldData.order);
                        }

                        break;
                    }
                }
            }
        }

        log.info("Generated discovery events on coordinator [vers=" + evts.keySet() + ", evts=" + evts + ']');

        ZKDiscoveryEvents newEvents;

        int expVer;

        if (curCrdEvts == null) {
            expVer = 0;

            newEvents = new ZKDiscoveryEvents(nextJoinOrder, newNodes, new TreeSet<>(evts.keySet()));
        }
        else {
//            TreeMap<Integer, ZKDiscoveryEvent> evts0 = new TreeMap<>(curCrdEvts.evts);
//
//            for (ZKDiscoveryEvent e : evts.values()) {
//                assert !evts0.containsKey(e.topVer) : "[newEvt=" + e + ", oldEvt=" + evts0.get(e.topVer) + ']';
//
//                evts0.put(e.topVer, e);
//            }
            TreeSet<Integer> evts0 = new TreeSet<>(curCrdEvts.evts);

            for (ZKDiscoveryEvent e : evts.values()) {
                assert !evts0.contains(e.topVer) : "[newEvt=" + e + ", oldEvts=" + evts0 + ']';

                evts0.add(e.topVer);
            }

            newEvents = new ZKDiscoveryEvents(nextJoinOrder, newNodes, evts0);

            expVer = curCrdEvts.ver;
        }

        try {
            // TODO ZK: handle case if node exists after crd change.
            for (ZKDiscoveryEvent evt : evts.values())
                zkCurator.create().withMode(CreateMode.PERSISTENT).forPath(DISCO_EVTS_HIST_PATH + "/" + evt.topVer, marshal(evt));
        }
        catch (Exception e) {
            U.error(log, "Failed to create discovery event node: " + e, e);
        }

        newEvents.ver = expVer + 1;

        try {
            zkCurator.setData().withVersion(expVer).forPath(EVENTS_PATH, marshal(newEvents));

            // zk.setData(EVENTS_PATH, marshal(newEvents), expVer);
        }
        catch (Exception e) {
            U.error(log, "Events update error: " + e, e);
        }

        curCrdEvts = newEvents;
    }

    /** */
    private ZKDiscoveryEvents curEvts;

    /** */
    private ZKDiscoveryEvents curCrdEvts;

    /** */
    private ZKDiscoveryEvent lastEvt;

    /** */
    private int lastProcessed = -1;

    /**
     *
     */
    class DataUpdateCallback implements AsyncCallback.DataCallback {
        @Override public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            log.info("Data changed [path=" + path + ", ver=" + (stat != null ? stat.getVersion() : null) + ']');

            if (data.length == 0)
                return;

            if (path.equals(EVENTS_PATH)) {
                assert stat != null;

                ZKDiscoveryEvents newEvts = unmarshal(data);

                newEvts.ver = stat.getVersion();

                for (Integer evtVer : newEvts.evts) {
                    if (evtVer > lastProcessed)
                        lastProcessed = evtVer;
                    else
                        continue;

                    boolean fireEvt;
                    boolean locJoin = false;

                    ZKDiscoveryEvent e;

                    try {
                        e = unmarshal(zkCurator.getData().forPath(DISCO_EVTS_HIST_PATH + "/" + evtVer));
                    }
                    catch (Exception err) {
                        U.error(log, "Failed to read discovery event: " + err, err);

                        continue;
                    }

                    if (lastEvt == null) {
                        locJoin = e.evtType == EventType.EVT_NODE_JOINED && e.node.id().equals(locNode.id());

                        if (locJoin) {
                            assert e.node.order() > 0 && e.node.internalOrder() > 0 : e.node;

                            locNode.internalOrder(e.node.internalOrder());
                            locNode.order(e.node.order());

                            fireEvt = true;
                        }
                        else
                            fireEvt = false;
                    }
                    else
                        fireEvt = e.topVer > lastEvt.topVer;

                    if (fireEvt) {
                        assert lastEvt == null || lastEvt.topVer + 1 == e.topVer : "lastEvt=" + lastEvt + ", nextEvt=" + e;

                        ZookeeperClusterNode evtNode = e.node;

                        if (!crd) {
                            synchronized (curTop) {
                                if (locJoin) {
                                    for (ZookeeperClusterNode node : e.allNodes) {
                                        assert node.internalOrder() > 0 : node;

                                        Object old = curTop.put(node.internalOrder(), node);

                                        assert old == null : node;
                                    }

                                    evtNode = locNode;

                                    DiscoveryDataBag dataBag = new DiscoveryDataBag(e.node.id());

                                    dataBag.joiningNodeData(e.joiningNodeData);
                                    dataBag.commonData(e.commonData);

                                    exchange.onExchange(dataBag);
                                }
                                else {
                                    switch (e.evtType) {
                                        case EventType.EVT_NODE_JOINED: {
                                            ZookeeperClusterNode node = e.node;

                                            DiscoveryDataBag dataBag = new DiscoveryDataBag(e.node.id());

                                            dataBag.joiningNodeData(e.joiningNodeData);
                                            dataBag.commonData(e.commonData);

                                            exchange.onExchange(dataBag);

                                            Object old = curTop.put(node.internalOrder(), node);

                                            evtNode = node;

                                            assert old == null : node;

                                            break;
                                        }

                                        case EventType.EVT_NODE_FAILED: {
                                            ZookeeperClusterNode node = e.node;

                                            evtNode = curTop.remove(node.internalOrder());

                                            assert evtNode != null : node;

                                            break;
                                        }

                                        default:
                                            assert false : e;
                                    }
                                }
                            }
                        }
                        else
                            evtNode = curTop.containsKey(e.node.internalOrder()) ? curTop.get(e.node.internalOrder()) : e.node;

                        log.info("Received discovery event, notify listener: " + e);

                        List<ClusterNode> allNodes = allNodesForEvent(e.allNodes);

                        evtNode.local(locNode.id().equals(evtNode.id()));

                        lsnr.onDiscovery(e.evtType, e.topVer, evtNode, allNodes, null, null);

                        if (locJoin) {
                            log.info("Local node joined: " + e);

                            joinLatch.countDown();

                            try {
                                String zkNode = JOIN_HIST_PATH + "/" + locNode.id().toString() + "-" + String.format("%010d", locNode.internalOrder() - 1);

                                zkCurator.delete().inBackground().forPath(zkNode);
                            }
                            catch (Exception err) {
                                U.error(log, "Failed to delete join history data");
                            }
                        }

                        lastEvt = e;
                    }
                }

                curEvts = newEvts;
            }
        }
    }

    private List<ClusterNode> allNodesForEvent(List<ZookeeperClusterNode> allNodes) {
        List<ClusterNode> res = new ArrayList<>(allNodes.size());

        for (int i = 0; i < allNodes.size(); i++) {
            ZookeeperClusterNode node = allNodes.get(i);

            ZookeeperClusterNode node0 = curTop.get(node.internalOrder());

            if (node0 != null)
                node = node0;

            node.local(locNode.id().equals(node.id()));

            res.add(node);
        }

        return res;
    }

    /**
     *
     */
    static class ZKDiscoveryEvents implements Serializable {
        /** */
        @GridToStringInclude
        int ver;

        /** */
        @GridToStringInclude
        final ZKAliveNodes aliveNodes;

        /** */
        @GridToStringInclude
        final TreeSet<Integer> evts;

        /** */
        final long nextJoinOrder;

        /**
         * @param aliveNodes
         * @param evts
         */
        ZKDiscoveryEvents(long nextJoinOrder, ZKAliveNodes aliveNodes, TreeSet<Integer> evts) {
            this.nextJoinOrder = nextJoinOrder;
            this.aliveNodes = aliveNodes;
            this.evts = evts;

            while (evts.size() > 1000)
                evts.remove(evts.first());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ZKDiscoveryEvents.class, this);
        }
    }

    /**
     *
     */
    static class ZKDiscoveryEvent implements Serializable {
        /** */
        @GridToStringInclude
        final int topVer;

        /** */
        @GridToStringInclude
        final int evtType;

        /** */
        @GridToStringInclude
        final ZookeeperClusterNode node;

        /** */
        @GridToStringExclude
        final List<ZookeeperClusterNode> allNodes;

        /** */
        Map<Integer, Serializable> joiningNodeData;

        /** */
        Map<Integer, Serializable> commonData;

        /**
         * @param evtType
         * @param topVer
         * @param node
         * @param allNodes
         */
        ZKDiscoveryEvent(int evtType, int topVer, ZookeeperClusterNode node, List<ZookeeperClusterNode> allNodes) {
            this.evtType = evtType;
            this.topVer = topVer;
            this.node = node;
            this.allNodes = allNodes;
        }

        /**
         *
         */
        void discoveryData(Map<Integer, Serializable> joiningNodeData, Map<Integer, Serializable> commonData) {
            this.joiningNodeData = joiningNodeData;
            this.commonData = commonData;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ZKDiscoveryEvent.class, this);
        }
    }


    /**
     *
     */
    private class ZookeeperWatcher implements Watcher {
        /** {@inheritDoc} */
        @Override public void process(WatchedEvent event) {
            IgniteLogger log = ZookeeperDiscoverySpi.this.log;

            if (log != null)
                log.info("Process event [type=" + event.getType() + ", state=" + event.getState() + ", path=" + event.getPath() + ']');

            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                zk.getChildren(event.getPath(), this, zkChildrenUpdateCallback, null);
            } else if (event.getType() == Event.EventType.NodeDataChanged) {
                zk.getData(event.getPath(), this, dataUpdateCallback, null);
            }
        }
    }

    /**
     *
     */
    private static class ZKClusterData implements Serializable {
        /** */
        private long gridStartTime;

        /**
         * @param gridStartTime
         */
        public ZKClusterData(long gridStartTime) {
            this.gridStartTime = gridStartTime;
        }
    }

    /**
     *
     */
    private static class ZKJoiningNodeData implements Serializable {
        /** */
        private final ZookeeperClusterNode node;

        /** */
        private final Map<Integer, Serializable> joiningNodeData;

        /**
         * @param node Node.
         * @param joiningNodeData Discovery data.
         */
        ZKJoiningNodeData(ZookeeperClusterNode node, Map<Integer, Serializable> joiningNodeData) {
            assert node != null && node.id() != null : node;
            assert joiningNodeData != null;

            this.node = node;
            this.joiningNodeData = joiningNodeData;
        }
    }
}
