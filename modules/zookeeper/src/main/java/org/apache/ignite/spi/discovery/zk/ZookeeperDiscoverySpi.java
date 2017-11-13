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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryForever;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.EventType;
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
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
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
public class ZookeeperDiscoverySpi extends IgniteSpiAdapter implements DiscoverySpi {
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
    private int sesTimeout = 5000;

    /** */
    private final ZookeeperWatcher zkWatcher;

    /** */
    private final JdkMarshaller marsh = new JdkMarshaller();

    /** */
    private final NodesUpdateCallback nodesUpdateCallback;

    /** */
    private final DataUpdateCallback dataUpdateCallback;

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

    /**
     *
     */
    public ZookeeperDiscoverySpi() {
        zkWatcher = new ZookeeperWatcher();

        nodesUpdateCallback = new NodesUpdateCallback();
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
        //throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId, @Nullable String warning) {
        // TODO ZK
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isClientMode() throws IllegalStateException {
        // TODO ZK
        return false;
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

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        try {
            initLocalNode();

            DiscoveryDataBag discoDataBag = new DiscoveryDataBag(locNode.id());

            exchange.collect(discoDataBag);

            String threadName = Thread.currentThread().getName();

            // ZK generates internal threads' names using current thread name.
            Thread.currentThread().setName("zk-" + igniteInstanceName);

            CuratorFramework c;

            try {
                c = CuratorFrameworkFactory.newClient(connectString, sesTimeout, sesTimeout, new RetryForever(500));

                c.start();

                zk = c.getZookeeperClient().getZooKeeper();
                // zk = new ZooKeeper(connectString, sesTimeout, zkWatcher);
            }
            finally {
                Thread.currentThread().setName(threadName);
            }

            for (;;) {
                boolean started = zk.exists(IGNITE_PATH, false) != null &&
                    zk.exists(ALIVE_NODES_PATH, false) != null &&
                    !zk.getChildren(ALIVE_NODES_PATH, false).isEmpty();

                if (!started) {
                    InterProcessMutex mux = new InterProcessMutex(c, IGNITE_INIT_LOCK_PATH);

                    mux.acquire();

                    try {
                        started = zk.exists(IGNITE_PATH, false) != null &&
                            zk.exists(ALIVE_NODES_PATH, false) != null &&
                            !zk.getChildren(ALIVE_NODES_PATH, false).isEmpty();

                        if (!started) {
                            log.info("First node starts, reset ZK state");

                            if (zk.exists(IGNITE_PATH, false) != null)
                                ZKUtil.deleteRecursive(zk, IGNITE_PATH);

                            // TODO ZK: properly handle first node start and init after full cluster restart.
                            if (zk.exists(IGNITE_PATH, false) == null) {
                                log.info("Initialize Zookeeper nodes.");

                                List<Op> initOps = new ArrayList<>();

                                ZKClusterData clusterData = new ZKClusterData(U.currentTimeMillis());

                                initOps.add(Op.create(IGNITE_PATH, EMPTY_BYTES, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                                initOps.add(Op.create(CLUSTER_PATH, marshal(clusterData), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                                initOps.add(Op.create(JOIN_HIST_PATH, EMPTY_BYTES, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                                initOps.add(Op.create(ALIVE_NODES_PATH, EMPTY_BYTES, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                                initOps.add(Op.create(EVENTS_PATH, EMPTY_BYTES, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));

                                zk.multi(initOps);
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

            ZKClusterData clusterData = unmarshal(zk.getData(CLUSTER_PATH, false, null));

            gridStartTime = clusterData.gridStartTime;

            zk.getData(EVENTS_PATH, zkWatcher, dataUpdateCallback, null);
            zk.getChildren(ALIVE_NODES_PATH, zkWatcher, nodesUpdateCallback, null);
            zk.getChildren(JOIN_HIST_PATH, zkWatcher, nodesUpdateCallback, null);

            List<Op> joinOps = new ArrayList<>();

            ZKJoiningNodeData joinData = new ZKJoiningNodeData(locNode, discoDataBag.joiningNodeData());

            byte[] nodeData = marshal(joinData);

            String zkNode = "/" + locNode.id().toString() + "-";

            joinOps.add(Op.create(JOIN_HIST_PATH + zkNode, nodeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL));
            joinOps.add(Op.create(ALIVE_NODES_PATH + zkNode, EMPTY_BYTES, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));

            List<OpResult> res = zk.multi(joinOps);

            log.info("Waiting for local join event [nodeId=" + locNode.id() + ", name=" + igniteInstanceName + ']');

            for(;;) {
                if (!joinLatch.await(10, TimeUnit.SECONDS)) {
                    U.warn(log, "Waiting for local join event [nodeId=" + locNode.id() + ", name=" + igniteInstanceName + ']');
                }
                else
                    break;
            }

        }
        catch (Exception e) {
            throw new IgniteSpiException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
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

        /** */
        final TreeMap<UUID, ZKNodeData> nodesById;

        /**
         * @param ver
         * @param nodesByOrder
         */
        ZKAliveNodes(int ver, TreeMap<Long, ZKNodeData> nodesByOrder) {
            this.ver = ver;
            this.nodesByOrder = nodesByOrder;

            nodesById = new TreeMap<>();

            for (ZKNodeData nodeData : nodesByOrder.values())
                nodesById.put(nodeData.nodeId, nodeData);
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
    private Map<Long, ZKNodeData> joinHist = new HashMap<>();

    /** */
    private boolean crd;

    /** */
    private ZKAliveNodes curAlive;

    /**
     *
     */
    class NodesUpdateCallback implements AsyncCallback.Children2Callback {
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            if (children == null || children.isEmpty())
                return;

            if (path.equals(JOIN_HIST_PATH)) {
                log.info("Join nodes changed [rc=" + rc +
                    ", path=" + path +
                    ", nodes=" + children +
                    ", ver=" + (stat != null ? stat.getCversion() : null) + ']');

                for (String child : children) {
                    ZKNodeData data = parseNodePath(child);

                    if (joinHist.put(data.order, data) == null) {
                        try {
                            log.info("New joined node data: " + data);

                            byte[] bytes = zk.getData(path + "/" + child, null, null);

                            assert bytes.length > 0;

                            ZKJoiningNodeData joinData = unmarshal(bytes);

                            assert joinData.node != null && joinData.joiningNodeData != null : joinData;

                            joinData.node.order(data.order);

                            data.joinData = joinData;
                        }
                        catch (Exception e) {
                            // TODO ZK
                            U.error(log, "Failed to get node data: " + e, e);
                        }
                    }
                }
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
    }

    /** */
    private final TreeMap<Long, ZookeeperClusterNode> curTop = new TreeMap<>();

    /**
     * @param oldNodes
     * @param newNodes
     */
    private void generateEvents(ZKAliveNodes oldNodes, ZKAliveNodes newNodes) {
        assert newNodes != null;

        ZKNodeData locNode = newNodes.nodesById.get(this.locNode.id());

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

        log.info("Generate discovery events [oldNodes=" + oldNodes + ", newNodes=" + newNodes + ']');

        if (oldNodes.ver == newNodes.ver)
            return;

        TreeMap<Integer, ZKDiscoveryEvent> evts = new TreeMap<>();

        Set<Long> failedNodes = new HashSet<>();
        Set<Long> joinedNodes = new HashSet<>();

        synchronized (curTop) {
            for (int v = oldNodes.ver + 1; v <= newNodes.ver; v++) {
                ZKNodeData joined = null;

                for (ZKNodeData newData : newNodes.nodesByOrder.values()) {
                    if (!curTop.containsKey(newData.order) && !joinedNodes.contains(newData.order)) {
                        joined = newData;

                        break;
                    }
                }

                // TODO ZK: process joinHist

                if (joined != null) {
                    joinedNodes.add(joined.order);

                    ZKNodeData data = joinHist.get(joined.order);

                    assert data != null : joined;

                    ZKJoiningNodeData joinData = data.joinData;

                    assert joinData != null : data;

                    curTop.put(joinData.node.order(), joinData.node);

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

                    if (!newNodes.nodesByOrder.containsKey(data.order)) {
                        v++;

                        ZookeeperClusterNode failedNode = curTop.remove(data.order);

                        assert failedNode != null : data.order;

                        log.info("ZK event [type=FAIL, node=" + failedNode.id() + ", ver=" + v + ']');

                        evts.put(v, new ZKDiscoveryEvent(EventType.EVT_NODE_FAILED,
                            v,
                            failedNode,
                            new ArrayList<>(curTop.values())));
                    }
                }
                else {
                    for (ZKNodeData oldData : oldNodes.nodesByOrder.values()) {
                        if (!failedNodes.contains(oldData.order) && !newNodes.nodesByOrder.containsKey(oldData.order)) {
                            failedNodes.add(oldData.order);

                            ZookeeperClusterNode failedNode = curTop.remove(oldData.order);

                            assert failedNode != null : oldData.order;

                            log.info("ZK event [type=FAIL, node=" + failedNode.id() + ", ver=" + v + ']');

                            evts.put(v, new ZKDiscoveryEvent(EventType.EVT_NODE_FAILED,
                                v,
                                failedNode,
                                new ArrayList<>(curTop.values())));

                            break;
                        }
                    }
                }
            }
        }

        log.info("Generated discovery events on coordinator [vers=" + evts.keySet() + ", evts=" + evts + ']');

        ZKDiscoveryEvents newEvents;

        int expVer;

        if (curCrdEvts == null) {
            expVer = 0;

            newEvents = new ZKDiscoveryEvents(newNodes, evts);
        }
        else {
            TreeMap<Integer, ZKDiscoveryEvent> evts0 = new TreeMap<>(curCrdEvts.evts);

            for (ZKDiscoveryEvent e : evts.values()) {
                assert !evts0.containsKey(e.topVer) : "[newEvt=" + e + ", oldEvt=" + evts0.get(e.topVer) + ']';

                evts0.put(e.topVer, e);
            }

            newEvents = new ZKDiscoveryEvents(newNodes, evts0);

            expVer = curCrdEvts.ver;
        }

        newEvents.ver = expVer + 1;

        try {
            zk.setData(EVENTS_PATH, marshal(newEvents), expVer);
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

                for (ZKDiscoveryEvent e : newEvts.evts.values()) {
                    boolean fireEvt;
                    boolean locJoin = false;

                    if (lastEvt == null) {
                        locJoin = e.evtType == EventType.EVT_NODE_JOINED && e.node.id().equals(locNode.id());

                        if (locJoin) {
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

                        if (!crd) {
                            synchronized (curTop) {
                                if (locJoin) {
                                    for (ZookeeperClusterNode node : e.allNodes) {
                                        assert node.order() > 0 : node;

                                        Object old = curTop.put(node.order(), node);

                                        assert old == null : node;
                                    }

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

                                            Object old = curTop.put(node.order(), node);

                                            assert old == null : node;

                                            break;
                                        }

                                        case EventType.EVT_NODE_FAILED: {
                                            ZookeeperClusterNode node = e.node;

                                            Object failedNode = curTop.remove(node.order());

                                            assert failedNode != null : node;

                                            break;
                                        }

                                        default:
                                            assert false : e;
                                    }
                                }
                            }
                        }

                        log.info("Received discovery event, notify listener: " + e);

                        List<ClusterNode> allNodes = allNodesForEvent(e.allNodes);

                        lsnr.onDiscovery(e.evtType, e.topVer, e.node, allNodes, null, null);

                        if (locJoin) {
                            log.info("Local node joined: " + e);

                            joinLatch.countDown();
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
        final TreeMap<Integer, ZKDiscoveryEvent> evts;

        /**
         * @param aliveNodes
         * @param evts
         */
        ZKDiscoveryEvents(ZKAliveNodes aliveNodes, TreeMap<Integer, ZKDiscoveryEvent> evts) {
            this.aliveNodes = aliveNodes;
            this.evts = evts;
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
            log.info("Process event [type=" + event.getType() + ", state=" + event.getState() + ", path=" + event.getPath() + ']');

            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                zk.getChildren(event.getPath(), this, nodesUpdateCallback, null);
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
