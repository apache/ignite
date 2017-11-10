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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
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
    private static final String CLUSTER_PATH = IGNITE_PATH + "/cluster";

    /** */
    private static final String EVENTS_PATH = CLUSTER_PATH + "/events";

    /** */
    private static final String JOIN_HIST_PATH = CLUSTER_PATH + "/joinHist";

    /** */
    private static final String ALIVE_NODES_PATH = CLUSTER_PATH + "/alive";

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
        // TODO
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
        // TODO
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
        // TODO
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
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(DiscoverySpiNodeAuthenticator auth) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public long getGridStartTime() {
        return gridStartTime;
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
        // TODO
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId, @Nullable String warning) {
        // TODO
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isClientMode() throws IllegalStateException {
        // TODO
        return false;
    }

    /**
     *
     */
    private void initLocalNode() {
        assert ignite != null;

        locNode = new ZookeeperClusterNode(ignite.configuration().getNodeId(),
            locNodeVer,
            locNodeAttrs,
            consistentId());

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

            zk = new ZooKeeper(connectString, sesTimeout, zkWatcher);

            // TODO: properly handle first node start and init after full cluster restart.
            if (zk.exists(IGNITE_PATH, false) == null) {
                log.info("Initialize Zookeeper nodes.");

                List<Op> initOps = new ArrayList<>();

                ZKClusterData clusterData = new ZKClusterData(U.currentTimeMillis());

                initOps.add(Op.create(IGNITE_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                initOps.add(Op.create(CLUSTER_PATH, marshal(clusterData), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                initOps.add(Op.create(JOIN_HIST_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                initOps.add(Op.create(ALIVE_NODES_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                initOps.add(Op.create(EVENTS_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));

                zk.multi(initOps);
            }

            ZKClusterData clusterData = unmarshal(zk.getData(CLUSTER_PATH, false, null));

            gridStartTime = clusterData.gridStartTime;

            zk.getData(EVENTS_PATH, true, dataUpdateCallback, null);
            zk.getChildren(ALIVE_NODES_PATH, true, nodesUpdateCallback, null);
            zk.getChildren(JOIN_HIST_PATH, true, nodesUpdateCallback, null);

            List<Op> joinOps = new ArrayList<>();

            byte[] nodeData = marshal(locNode);

            String zkNode = "/" + locNode.id().toString() + "-";

            joinOps.add(Op.create(JOIN_HIST_PATH + zkNode, nodeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL));
            joinOps.add(Op.create(ALIVE_NODES_PATH + zkNode, nodeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));

            List<OpResult> res = zk.multi(joinOps);

            log.info("Waiting for local join event.");

            joinLatch.await();
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
        transient ZookeeperClusterNode clusterNode;

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
                            byte[] nodeData = zk.getData(path + "/" + child, null, null);

                            assert nodeData.length > 0;

                            data.clusterNode = unmarshal(nodeData);

                            data.clusterNode.order(data.order);
                        }
                        catch (Exception e) {
                            // TODO
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

        long nextJoinOrder = oldNodes.nodesByOrder.isEmpty() ? 1 : oldNodes.nodesByOrder.lastKey() + 1;

        TreeMap<Integer, ZKDiscoveryEvent> evts = new TreeMap<>();

        Set<Long> failed = new HashSet<>();

        synchronized (curTop) {
            for (int v = oldNodes.ver + 1; v <= newNodes.ver; v++) {
                ZKNodeData data = joinHist.get(nextJoinOrder);

                if (data != null) {
                    curTop.put(data.clusterNode.order(), data.clusterNode);

                    evts.put(v, new ZKDiscoveryEvent(EventType.EVT_NODE_JOINED,
                        v,
                        data.clusterNode,
                        new ArrayList<>(curTop.values())));

                    if (!newNodes.nodesByOrder.containsKey(data.order)) {
                        v++;

                        ZookeeperClusterNode failedNode = curTop.remove(data.order);

                        assert failedNode != null : data.order;

                        evts.put(v, new ZKDiscoveryEvent(EventType.EVT_NODE_FAILED,
                            v,
                            failedNode,
                            new ArrayList<>(curTop.values())));
                    }

                    nextJoinOrder++;
                }
                else {
                    for (ZKNodeData oldData : oldNodes.nodesByOrder.values()) {
                        if (!failed.contains(oldData.order) && !newNodes.nodesByOrder.containsKey(oldData.order)) {
                            failed.add(oldData.order);

                            ZookeeperClusterNode failedNode = curTop.remove(oldData.order);

                            assert failedNode != null : oldData.order;

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

        log.info("Generated discovery events on coordinator: " + evts);

        ZKDiscoveryEvents newEvents;

        int expVer;

        if (curCrdEvts == null) {
            expVer = 0;

            newEvents = new ZKDiscoveryEvents(newNodes, evts);
        }
        else {
            TreeMap<Integer, ZKDiscoveryEvent> evts0 = new TreeMap<>(curCrdEvts.evts);

            evts0.putAll(evts);

            newEvents = new ZKDiscoveryEvents(newNodes, evts);

            expVer = curCrdEvts.ver;
        }

        newEvents.ver = expVer + 1;

        try {
            zk.setData(EVENTS_PATH, marshal(newEvents), expVer);
        }
        catch (Exception e) {
            log.info("Events update error: " + e);

            e.printStackTrace(System.out);
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
                        locNode.order(e.node.order());

                        locJoin = e.evtType == EventType.EVT_NODE_JOINED && e.node.id().equals(locNode.id());

                        fireEvt = locJoin;
                    }
                    else
                        fireEvt = e.topVer > lastEvt.topVer;

                    if (fireEvt) {
                        assert lastEvt == null || lastEvt.topVer + 1 == e.topVer : "lastEvt=" + lastEvt + ", nextEvt=" + e;

                        if (!crd) {
                            if (locJoin) {
                                for (ZookeeperClusterNode node : e.allNodes) {
                                    assert node.order() > 0 : node;

                                    Object old = curTop.put(node.order(), node);

                                    assert old == null : node;
                                }
                            }
                            else {
                                switch (e.evtType) {
                                    case EventType.EVT_NODE_JOINED: {
                                        ZookeeperClusterNode node = e.node;

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
        final int evtType;

        /** */
        @GridToStringInclude
        final ZookeeperClusterNode node;

        /** */
        @GridToStringInclude
        final List<ZookeeperClusterNode> allNodes;

        /** */
        @GridToStringInclude
        final int topVer;

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
                zk.getChildren(event.getPath(), true, nodesUpdateCallback, null);
            } else if (event.getType() == Event.EventType.NodeDataChanged) {
                zk.getData(event.getPath(), true, dataUpdateCallback, null);
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
}
