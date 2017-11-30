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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiThread;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;

/**
 * TODO ZK: check if compression makes sense.
 */
public class ZookeeperDiscoveryImpl {
    /** */
    static final String IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD = "IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD";

    /** */
    private final String igniteInstanceName;

    /** */
    private final String connectString;

    /** */
    private final int sesTimeout;

    /** */
    private final JdkMarshaller marsh = new JdkMarshaller();

    /** */
    private final ZkIgnitePaths zkPaths;

    /** */
    private final IgniteLogger log;

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** */
    private final ZookeeperClusterNode locNode;

    /** */
    private final DiscoverySpiListener lsnr;

    /** */
    private final DiscoverySpiDataExchange exchange;

    /** */
    private final boolean clientReconnectEnabled;

    /** */
    private final GridFutureAdapter<Void> joinFut = new GridFutureAdapter<>();

    /** */
    private final AliveNodeDataWatcher aliveNodeDataWatcher = new AliveNodeDataWatcher();

    /** */
    private final ZkWatcher watcher;

    /** */
    private final ZKChildrenCallback childrenCallback;

    /** */
    private final ZkDataCallback dataCallback;

    /** */
    private final int evtsAckThreshold;

    /** */
    private ZkRuntimeState state;

    /** */
    private volatile ConnectionState connState = ConnectionState.STARTED;

    /** */
    private final AtomicBoolean stop = new AtomicBoolean();

    /** */
    private final Object stateMux = new Object();

    /**
     * @param log Logger.
     * @param zkRootPath Zookeeper base path node all nodes.
     * @param locNode Local node instance.
     * @param lsnr Discovery events listener.
     * @param exchange Discovery data exchange.
     */
    public ZookeeperDiscoveryImpl(
        String igniteInstanceName,
        String connectString,
        int sesTimeout,
        IgniteLogger log,
        String zkRootPath,
        ZookeeperClusterNode locNode,
        DiscoverySpiListener lsnr,
        DiscoverySpiDataExchange exchange,
        boolean clientReconnectEnabled) {
        assert locNode.id() != null && locNode.isLocal() : locNode;

        MarshallerUtils.setNodeName(marsh, igniteInstanceName);

        ZkIgnitePaths.validatePath(zkRootPath);

        zkPaths = new ZkIgnitePaths(zkRootPath);

        this.igniteInstanceName = igniteInstanceName;
        this.connectString = connectString;
        this.sesTimeout = sesTimeout;
        this.log = log.getLogger(getClass());
        this.locNode = locNode;
        this.lsnr = lsnr;
        this.exchange = exchange;
        this.clientReconnectEnabled = clientReconnectEnabled;

        watcher = new ZkWatcher();
        childrenCallback = new ZKChildrenCallback();
        dataCallback = new ZkDataCallback();

        int evtsAckThreshold = IgniteSystemProperties.getInteger(IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD, 5);

        if (evtsAckThreshold <= 0)
            evtsAckThreshold = 1;

        this.evtsAckThreshold = evtsAckThreshold;
    }

    /**
     * @return Logger.
     */
    IgniteLogger log() {
        return log;
    }

    /**
     * @return Local node instance.
     */
    public ClusterNode localNode() {
        return locNode;
    }

    /**
     * @param nodeId Node ID.
     * @return Node instance.
     */
    @Nullable public ClusterNode node(UUID nodeId) {
        assert nodeId != null;

        return state.top.nodesById.get(nodeId);
    }

    /**
     * @param nodeId Node ID.
     * @return Ping result.
     */
    public boolean pingNode(UUID nodeId) {
        // TODO ZK
        checkState();

        return node(nodeId) != null;
    }

    /**
     *
     */
    public void reconnect() {
        assert clientReconnectEnabled;

        synchronized (stateMux) {
            if (connState == ConnectionState.STARTED)
                connState = ConnectionState.DISCONNECTED;
            else
                return;
        }

        state.zkClient.onCloseStart();

        busyLock.block();

        busyLock.unblock();

        state.zkClient.close();

        UUID newId = UUID.randomUUID();

        U.quietAndWarn(log, "Local node will try to reconnect to cluster with new id due to network problems [" +
            "newId=" + newId +
            ", prevId=" + locNode.id() +
            ", locNode=" + locNode + ']');

        doReconnect(newId);
    }

    /**
     * @param newId New ID.
     */
    private void doReconnect(UUID newId) {
        locNode.onClientDisconnected(newId);

        if (state.joined) {
            assert state.evtsData != null;

            lsnr.onDiscovery(EVT_CLIENT_NODE_DISCONNECTED,
                state.evtsData.topVer,
                locNode,
                state.top.topologySnapshot(),
                Collections.<Long, Collection<ClusterNode>>emptyMap(),
                null);
        }

        try {
            joinTopology0(state.joined);
        }
        catch (Exception e) {
            U.error(log, "Failed to reconnect: " + e, e);

            onSegmented(e);
        }
    }


    /**
     * @param e Error.
     */
    private void onSegmented(Exception e) {
        if (state.joined) {
            synchronized (stateMux) {
                connState = ConnectionState.STOPPED;
            }

            zkClient().zk().sync(zkPaths.clusterDir, new SegmentedWatcher(), null);
        }
        else
            joinFut.onDone(e);
    }

    /**
     *
     */
    class SegmentedWatcher implements AsyncCallback.VoidCallback {
        @Override public void processResult(int rc, String path, Object ctx) {
            assert state.evtsData != null;

            lsnr.onDiscovery(EventType.EVT_NODE_SEGMENTED,
                state.evtsData.topVer,
                locNode,
                state.top.topologySnapshot(),
                Collections.<Long, Collection<ClusterNode>>emptyMap(),
                null);
        }
    }

    /**
     * @return Remote nodes.
     */
    public Collection<ClusterNode> remoteNodes() {
        checkState();

        return state.top.remoteNodes();
    }

    /**
     *
     */
    private void checkState() {
        switch (connState) {
            case STARTED:
                break;

            case STOPPED:
                throw new IgniteSpiException("Zookeeper client closed.");

            case DISCONNECTED:
                throw new IgniteClientDisconnectedException(null, "Client is disconnected.");
        }
    }

    /**
     * @param nodeId Node ID.
     * @return {@code True} if node joined or joining topology.
     */
    public boolean knownNode(UUID nodeId) {
        while (!busyLock.enterBusy())
            checkState();

        try {
            List<String> children = state.zkClient.getChildren(zkPaths.aliveNodesDir);

            for (int i = 0; i < children.size(); i++) {
                UUID id = ZkIgnitePaths.aliveNodeId(children.get(i));

                if (nodeId.equals(id))
                    return true;
            }

            return false;
        }
        catch (ZookeeperClientFailedException e) {
            throw new IgniteException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param msg Message.
     */
    public void sendCustomMessage(DiscoverySpiCustomMessage msg) {
        assert msg != null;

        byte[] msgBytes;

        try {
            msgBytes = U.zip(marshal(msg));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal custom message: " + msg, e);
        }

        while (!busyLock.enterBusy())
            checkState();

        try {
            String prefix = UUID.randomUUID().toString();

            state.zkClient.createSequential(prefix,
                zkPaths.customEvtsDir,
                prefix + ":" + locNode.id() + '|',
                msgBytes,
                CreateMode.PERSISTENT_SEQUENTIAL);
        }
        catch (ZookeeperClientFailedException e) {
            throw new IgniteException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @return Cluster start time.
     */
    public long gridStartTime() {
        return state.gridStartTime;
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    public void joinTopology() throws InterruptedException {
        joinTopology0(false);

        for (;;) {
            try {
                joinFut.get(10_000);

                break;
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                U.warn(log, "Waiting for local join event [nodeId=" + locNode.id() + ", name=" + igniteInstanceName + ']');
            }
            catch (Exception e) {
                throw new IgniteSpiException("Failed to join cluster", e);
            }
        }
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    private void joinTopology0(boolean prevJoined) throws InterruptedException {
        state = new ZkRuntimeState(prevJoined);

        DiscoveryDataBag discoDataBag = new DiscoveryDataBag(locNode.id());

        exchange.collect(discoDataBag);

        ZkJoiningNodeData joinData = new ZkJoiningNodeData(locNode, discoDataBag.joiningNodeData());

        byte[] joinDataBytes;

        try {
            joinDataBytes = U.zip(marshal(joinData));
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to marshal joining node data", e);
        }

        try {
            state.zkClient = new ZookeeperClient(igniteInstanceName,
                log,
                connectString,
                sesTimeout,
                new ConnectionLossListener());
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to create Zookeeper client", e);
        }

        startJoin(joinDataBytes);
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    private void initZkNodes() throws InterruptedException {
        try {
            if (state.zkClient.exists(zkPaths.aliveNodesDir))
                return; // This path is created last, assume all others dirs are created.

            List<String> dirs = new ArrayList<>();

            // TODO ZK: test create all parents?

            dirs.add(zkPaths.clusterDir);
            dirs.add(zkPaths.evtsPath);
            dirs.add(zkPaths.joinDataDir);
            dirs.add(zkPaths.customEvtsDir);
            dirs.add(zkPaths.customEvtsAcksDir);
            dirs.add(zkPaths.aliveNodesDir);

            try {
                state.zkClient.createAll(dirs, PERSISTENT);
            }
            catch (KeeperException.NodeExistsException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to create nodes using bulk operation: " + e);

                for (String dir : dirs)
                    state.zkClient.createIfNeeded(dir, null, PERSISTENT);
            }
        }
        catch (ZookeeperClientFailedException e) {
            throw new IgniteSpiException("Failed to initialize Zookeeper nodes", e);
        }
    }

    /**
     * @param joinDataBytes Joining node data.
     * @throws InterruptedException If interrupted.
     */
    private void startJoin(byte[] joinDataBytes) throws InterruptedException {
        if (!busyLock.enterBusy())
            return;

        try {
            initZkNodes();

            String prefix = UUID.randomUUID().toString();

            // TODO ZK: handle max size.

            String path = state.zkClient.createSequential(prefix,
                zkPaths.joinDataDir,
                prefix + ":" + locNode.id() + "|",
                joinDataBytes,
                EPHEMERAL_SEQUENTIAL);

            int seqNum = Integer.parseInt(path.substring(path.lastIndexOf('|') + 1));

            state.locNodeZkPath = state.zkClient.createSequential(
                prefix,
                zkPaths.aliveNodesDir,
                prefix + ":" + locNode.id() + "|" + seqNum + "|",
                null,
                EPHEMERAL_SEQUENTIAL);

            log.info("Node started join [nodeId=" + locNode.id() +
                ", instanceName=" + locNode.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME) +
                ", joinDataSize=" + joinDataBytes.length +
                ", nodePath=" + state.locNodeZkPath + ']');

            state.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, null, new CheckCoordinatorCallback());

            state.zkClient.getDataAsync(zkPaths.evtsPath, watcher, dataCallback);
        }
        catch (ZookeeperClientFailedException e) {
            throw new IgniteSpiException("Failed to initialize Zookeeper nodes", e);
        }
        finally {
            busyLock.leaveBusy();

            connStartLatch.countDown();
        }
    }

    /** TODO ZK */
    private final CountDownLatch connStartLatch = new CountDownLatch(1);

    /**
     * For testing only.
     *
     * @throws Exception If failed.
     */
    void waitConnectStart() throws Exception {
        connStartLatch.await();
    }

    /**
     * @param rc Callback result code.
     * @param aliveNodes Alive nodes.
     * @throws Exception If failed.
     */
    private void checkIsCoordinator(int rc, final List<String> aliveNodes) throws Exception {
        assert rc == 0 : KeeperException.Code.get(rc);

        TreeMap<Integer, String> alives = new TreeMap<>();

        Integer locInternalId = null;

        for (String aliveNodePath : aliveNodes) {
            Integer internalId = ZkIgnitePaths.aliveInternalId(aliveNodePath);

            alives.put(internalId, aliveNodePath);

            if (locInternalId == null) {
                UUID nodeId = ZkIgnitePaths.aliveNodeId(aliveNodePath);

                if (locNode.id().equals(nodeId))
                    locInternalId = internalId;
            }
        }

        assert !alives.isEmpty();
        assert locInternalId != null;

        Map.Entry<Integer, String> crdE = alives.firstEntry();

        if (locInternalId.equals(crdE.getKey()))
            onBecomeCoordinator(aliveNodes, locInternalId);
        else {
            assert alives.size() > 1;

            Map.Entry<Integer, String> prevE = alives.floorEntry(locInternalId - 1);

            assert prevE != null;

            log.info("Discovery coordinator already exists, watch for previous node [" +
                "locId=" + locNode.id() +
                ", prevPath=" + prevE.getValue() + ']');

            PreviousNodeWatcher watcher = new PreviousNodeWatcher();

            state.zkClient.existsAsync(zkPaths.aliveNodesDir + "/" + prevE.getValue(), watcher, watcher);
        }
    }

    private void onPreviousNodeFail() throws Exception {
        // TODO ZK:
//        if (locInternalId == crdInternalId + 1) {
//            if (log.isInfoEnabled())
//                log.info("Previous discovery coordinator failed [locId=" + locNode.id() + ']');
//
//            onBecomeCoordinator(aliveNodes, locInternalId);
//        }
        if (log.isInfoEnabled())
            log.info("Previous node failed, check is node new coordinator [locId=" + locNode.id() + ']');

        state.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, null, new CheckCoordinatorCallback());
    }

    /**
     * @param aliveNodes Alive nodes paths.
     * @param locInternalId Local node's internal ID.
     * @throws Exception If failed.
     */
    private void onBecomeCoordinator(List<String> aliveNodes, int locInternalId) throws Exception {
        byte[] evtsDataBytes = state.zkClient.getData(zkPaths.evtsPath);

        if (evtsDataBytes.length > 0)
            processNewEvents(evtsDataBytes);

        state.crd = true;

        if (state.joined) {
            if (log.isInfoEnabled())
                log.info("Node is new discovery coordinator [locId=" + locNode.id() + ']');

            assert locNode.order() > 0 : locNode;
            assert state.evtsData != null;

            for (ZkDiscoveryEventData evtData : state.evtsData.evts.values())
                evtData.initRemainingAcks(state.top.nodesByOrder.values(), null);

            handleProcessedEvents("crd", null);
        }
        else {
            if (log.isInfoEnabled())
                log.info("Node is first cluster node [locId=" + locNode.id() + ']');

            newClusterStarted(locInternalId);
        }

        state.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, watcher, childrenCallback);
        state.zkClient.getChildrenAsync(zkPaths.customEvtsDir, watcher, childrenCallback);

        for (String alivePath : aliveNodes)
            watchAliveNodeData(alivePath);
    }

    /**
     * @param alivePath
     */
    private void watchAliveNodeData(String alivePath) {
        assert state.locNodeZkPath != null;

        String path = zkPaths.aliveNodesDir + "/" + alivePath;

        if (!path.equals(state.locNodeZkPath))
            state.zkClient.getDataAsync(path, aliveNodeDataWatcher, aliveNodeDataWatcher);
    }

    /**
     * @param aliveNodes ZK nodes representing alive cluster nodes.
     * @throws Exception If failed.
     */
    private void generateTopologyEvents(List<String> aliveNodes) throws Exception {
        assert state.crd;

        if (log.isInfoEnabled())
            log.info("Process alive nodes change: " + aliveNodes.size());

        TreeMap<Integer, String> alives = new TreeMap<>();

        TreeMap<Long, ZookeeperClusterNode> curTop = new TreeMap<>(state.top.nodesByOrder);

        boolean newEvts = false;

        for (String child : aliveNodes) {
            Integer internalId = ZkIgnitePaths.aliveInternalId(child);

            Object old = alives.put(internalId, child);

            assert old == null;

            if (!state.top.nodesByInternalId.containsKey(internalId)) {
                generateNodeJoin(curTop, internalId, child);

                watchAliveNodeData(child);

                newEvts = true;
            }
        }

        for (Map.Entry<Integer, ZookeeperClusterNode> e : state.top.nodesByInternalId.entrySet()) {
            if (!alives.containsKey(e.getKey())) {
                ZookeeperClusterNode failedNode = e.getValue();

                handleProcessedEventsOnNodeFail(failedNode, alives);

                generateNodeFail(curTop, failedNode);

                newEvts = true;
            }
        }

        if (newEvts)
            saveAndProcessNewEvents();
    }

    /**
     * @throws Exception If failed.
     */
    private void saveAndProcessNewEvents() throws Exception {
        long start = System.currentTimeMillis();

        byte[] evtsBytes = U.zip(marshal(state.evtsData));

        state.zkClient.setData(zkPaths.evtsPath, evtsBytes, -1);

        long time = System.currentTimeMillis() - start;

        if (log.isInfoEnabled()) {
            log.info("Discovery coordinator saved new topology events [topVer=" + state.evtsData.topVer +
                ", size=" + evtsBytes.length +
                ", evts=" + state.evtsData.evts.size() +
                ", lastEvt=" + state.evtsData.evtIdGen +
                ", saveTime=" + time + ']');
        }

        processNewEvents(state.evtsData);
    }

    /**
     * @param curTop Current topology.
     * @param failedNode Failed node.
     */
    private void generateNodeFail(TreeMap<Long, ZookeeperClusterNode> curTop, ZookeeperClusterNode failedNode) {
        Object rmvd = curTop.remove(failedNode.order());

        assert rmvd != null;

        state.evtsData.topVer++;
        state.evtsData.evtIdGen++;

        ZkDiscoveryNodeFailEventData evtData = new ZkDiscoveryNodeFailEventData(
            state.evtsData.evtIdGen,
            state.evtsData.topVer,
            failedNode.internalId());

        state.evtsData.addEvent(curTop.values(), evtData, null);

        if (log.isInfoEnabled())
            log.info("Generated NODE_FAILED event [evt=" + evtData + ']');
    }

    /**
     * @param curTop Current nodes.
     * @param internalId Joined node internal ID.
     * @param aliveNodePath Joined node path.
     * @throws Exception If failed.
     */
    private void generateNodeJoin(TreeMap<Long, ZookeeperClusterNode> curTop,
        int internalId,
        String aliveNodePath)
        throws Exception
    {
        UUID nodeId = ZkIgnitePaths.aliveNodeId(aliveNodePath);

        String joinDataPath = zkPaths.joiningNodeDataPath(nodeId, aliveNodePath);
        byte[] joinData;

        try {
            joinData = state.zkClient.getData(joinDataPath);
        }
        catch (KeeperException.NoNodeException e) {
            U.warn(log, "Failed to read joining node data, node left before join process finished: " + nodeId);

            return;
        }

        // TODO ZK: fail node if can not unmarshal.
        ZkJoiningNodeData joiningNodeData = unmarshalZip(joinData);

        ZookeeperClusterNode joinedNode = joiningNodeData.node();

        assert nodeId.equals(joinedNode.id()) : joiningNodeData.node();

        state.evtsData.topVer++;
        state.evtsData.evtIdGen++;

        joinedNode.order(state.evtsData.topVer);
        joinedNode.internalId(internalId);

        DiscoveryDataBag joiningNodeBag = new DiscoveryDataBag(nodeId);

        joiningNodeBag.joiningNodeData(joiningNodeData.discoveryData());

        exchange.onExchange(joiningNodeBag);

        DiscoveryDataBag collectBag = new DiscoveryDataBag(nodeId, new HashSet<Integer>());

        exchange.collect(collectBag);

        Map<Integer, Serializable> commonData = collectBag.commonData();

        ZkJoinEventDataForJoined dataForJoined = new ZkJoinEventDataForJoined(
            new ArrayList<>(curTop.values()),
            commonData);

        Object old = curTop.put(joinedNode.order(), joinedNode);

        assert old == null;

        ZkDiscoveryNodeJoinEventData evtData = new ZkDiscoveryNodeJoinEventData(
            state.evtsData.evtIdGen,
            state.evtsData.topVer,
            joinedNode.id(),
            joinedNode.internalId());

        evtData.joiningNodeData = joiningNodeData;

        state.evtsData.addEvent(dataForJoined.topology(), evtData, null);

        evtData.addRemainingAck(joinedNode); // Topology for joined node does not contain joined node.

        byte[] dataForJoinedBytes = U.zip(marshal(dataForJoined));

        long start = System.currentTimeMillis();

        state.zkClient.createIfNeeded(zkPaths.joinEventDataPath(evtData.eventId()), joinData, PERSISTENT);
        state.zkClient.createIfNeeded(zkPaths.joinEventDataPathForJoined(evtData.eventId()), dataForJoinedBytes, PERSISTENT);

        long time = System.currentTimeMillis() - start;

        if (log.isInfoEnabled()) {
            log.info("Generated NODE_JOINED event [evt=" + evtData +
                ", joinedDataSize=" + joinData.length +
                ", dataForJoinedSize=" + dataForJoinedBytes.length +
                ", addDataTime=" + time + ']');
        }
    }

    /**
     * @param locInternalId Local node internal ID.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void newClusterStarted(int locInternalId) throws Exception {
        cleanupPreviousClusterData();

        state.joined = true;

        state.gridStartTime = U.currentTimeMillis();

        state.evtsData = new ZkDiscoveryEventsData(state.gridStartTime, 1L, new TreeMap<Long, ZkDiscoveryEventData>());

        locNode.internalId(locInternalId);
        locNode.order(1);

        state.top.addNode(locNode);

        String path = state.locNodeZkPath.substring(state.locNodeZkPath.lastIndexOf('/') + 1);

        String joinDataPath = zkPaths.joiningNodeDataPath(locNode.id(), path);

        if (log.isInfoEnabled())
            log.info("Delete join data: " + joinDataPath);

        state.zkClient.deleteIfExistsAsync(joinDataPath);

        final List<ClusterNode> topSnapshot = Collections.singletonList((ClusterNode)locNode);

        if (connState == ConnectionState.DISCONNECTED)
            connState = ConnectionState.STARTED;

        lsnr.onDiscovery(EventType.EVT_NODE_JOINED,
            1L,
            locNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            null);

        if (state.prevJoined) {
            lsnr.onDiscovery(EVT_CLIENT_NODE_RECONNECTED,
                1L,
                locNode,
                topSnapshot,
                Collections.<Long, Collection<ClusterNode>>emptyMap(),
                null);

            U.quietAndWarn(log, "Client node was reconnected after it was already considered failed.");
        }

        joinFut.onDone();
    }

    /**
     * @throws Exception If failed.
     */
    private void cleanupPreviousClusterData() throws Exception {
        // TODO ZK: use multi, better batching.
        state.zkClient.setData(zkPaths.evtsPath, null, -1);

        List<String> evtChildren = state.zkClient.getChildren(zkPaths.evtsPath);

        for (String evtPath : evtChildren) {
            String evtDir = zkPaths.evtsPath + "/" + evtPath;

            removeChildren(evtDir);
        }

        state.zkClient.deleteAll(zkPaths.evtsPath, evtChildren, -1);

        state.zkClient.deleteAll(zkPaths.customEvtsDir,
            state.zkClient.getChildren(zkPaths.customEvtsDir),
            -1);

        state.zkClient.deleteAll(zkPaths.customEvtsAcksDir,
            state.zkClient.getChildren(zkPaths.customEvtsAcksDir),
            -1);
    }

    /**
     * @param path Path.
     * @throws Exception If failed.
     */
    private void removeChildren(String path) throws Exception {
        state.zkClient.deleteAll(path, state.zkClient.getChildren(path), -1);
    }

    ZkClusterNodes nodes() {
        return state.top;
    }

    ZookeeperClient zkClient() {
        return state.zkClient;
    }

    /**
     * @param customEvtNodes ZK nodes representing custom events to process.
     * @throws Exception If failed.
     */
    private void generateCustomEvents(List<String> customEvtNodes) throws Exception {
        assert state.crd;

        TreeMap<Integer, String> newEvts = null;

        for (int i = 0; i < customEvtNodes.size(); i++) {
            String evtPath = customEvtNodes.get(i);

            int evtSeq = ZkIgnitePaths.customEventSequence(evtPath);

            if (evtSeq > state.evtsData.procCustEvt) {
                if (newEvts == null)
                    newEvts = new TreeMap<>();

                newEvts.put(evtSeq, evtPath);
            }
        }

        if (newEvts != null) {
            for (Map.Entry<Integer, String> evtE : newEvts.entrySet()) {
                UUID sndNodeId = ZkIgnitePaths.customEventSendNodeId(evtE.getValue());

                ZookeeperClusterNode sndNode = state.top.nodesById.get(sndNodeId);

                String evtDataPath = zkPaths.customEvtsDir + "/" + evtE.getValue();

                if (sndNode != null) {
                    byte[] evtBytes = state.zkClient.getData(zkPaths.customEvtsDir + "/" + evtE.getValue());

                    DiscoverySpiCustomMessage msg;

                    try {
                        msg = unmarshalZip(evtBytes);

                        state.evtsData.evtIdGen++;

                        ZkDiscoveryCustomEventData evtData = new ZkDiscoveryCustomEventData(
                            state.evtsData.evtIdGen,
                            state.evtsData.topVer,
                            sndNodeId,
                            evtE.getValue(),
                            false);

                        evtData.msg = msg;

                        state.evtsData.addEvent(state.top.nodesByOrder.values(), evtData, null);

                        if (log.isDebugEnabled())
                            log.debug("Generated CUSTOM event [evt=" + evtData + ", msg=" + msg + ']');
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to unmarshal custom discovery message: " + e, e);
                    }
                }
                else {
                    U.warn(log, "Ignore custom event from unknown node: " + sndNodeId);

                    state.zkClient.deleteIfExistsAsync(evtDataPath);
                }

                state.evtsData.procCustEvt = evtE.getKey();
            }

            saveAndProcessNewEvents();
        }
    }

    /**
     * @param data Marshalled events.
     * @throws Exception If failed.
     */
    private void processNewEvents(byte[] data) throws Exception {
        if (data.length == 0)
            return;

        assert !state.crd;

        ZkDiscoveryEventsData newEvts = unmarshalZip(data);

        // Need keep processed custom events since they contains message object.
        if (state.evtsData != null) {
            for (Map.Entry<Long, ZkDiscoveryEventData> e : state.evtsData.evts.entrySet()) {
                ZkDiscoveryEventData evtData = e.getValue();

                if (evtData.eventType() == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
                    ZkDiscoveryCustomEventData evtData0 =
                        (ZkDiscoveryCustomEventData)newEvts.evts.get(evtData.eventId());

                    if (evtData0 != null)
                        evtData0.msg = ((ZkDiscoveryCustomEventData)evtData).msg;
                }
            }
        }

        processNewEvents(newEvts);

        state.evtsData = newEvts;
    }

    /**
     * @param evtsData Events.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processNewEvents(final ZkDiscoveryEventsData evtsData) throws Exception {
        TreeMap<Long, ZkDiscoveryEventData> evts = evtsData.evts;

        boolean updateNodeInfo = false;

        for (ZkDiscoveryEventData evtData : evts.tailMap(state.locNodeInfo.lastProcEvt, false).values()) {
            if (!state.joined) {
                if (evtData.eventType() != EventType.EVT_NODE_JOINED)
                    continue;

                ZkDiscoveryNodeJoinEventData evtData0 = (ZkDiscoveryNodeJoinEventData)evtData;

                UUID joinedId = evtData0.nodeId;

                boolean locJoin = evtData.eventType() == EventType.EVT_NODE_JOINED &&
                    locNode.id().equals(joinedId);

                if (locJoin)
                    processLocalJoin(evtsData, evtData0);
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("New discovery event data [evt=" + evtData + ", evtsHist=" + evts.size() + ']');

                switch (evtData.eventType()) {
                    case EventType.EVT_NODE_JOINED: {
                        ZkDiscoveryNodeJoinEventData evtData0 = (ZkDiscoveryNodeJoinEventData)evtData;

                        ZkJoiningNodeData joiningData;

                        if (state.crd) {
                            assert evtData0.joiningNodeData != null;

                            joiningData = evtData0.joiningNodeData;
                        }
                        else {
                            String path = zkPaths.joinEventDataPath(evtData.eventId());

                            joiningData = unmarshalZip(state.zkClient.getData(path));

                            DiscoveryDataBag dataBag = new DiscoveryDataBag(evtData0.nodeId);

                            dataBag.joiningNodeData(joiningData.discoveryData());

                            exchange.onExchange(dataBag);
                        }

                        notifyNodeJoin(evtData0, joiningData);

                        break;
                    }

                    case EventType.EVT_NODE_FAILED: {
                        notifyNodeFail((ZkDiscoveryNodeFailEventData)evtData);

                        break;
                    }

                    case DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT: {
                        ZkDiscoveryCustomEventData evtData0 = (ZkDiscoveryCustomEventData)evtData;

                        if (evtData0.ackEvent() && evtData0.topologyVersion() < locNode.order())
                            break;

                        DiscoverySpiCustomMessage msg;

                        if (state.crd) {
                            assert evtData0.msg != null : evtData0;

                            msg = evtData0.msg;
                        }
                        else {
                            String path;

                            if (evtData0.ackEvent())
                                path = zkPaths.ackEventDataPath(evtData0.eventId());
                            else
                                path = zkPaths.customEventDataPath(false, evtData0.evtPath);

                            msg = unmarshalZip(state.zkClient.getData(path));

                            evtData0.msg = msg;
                        }

                        notifyCustomEvent(evtData0, msg);

                        if (!evtData0.ackEvent())
                            updateNodeInfo = true;

                        break;
                    }

                    default:
                        assert false : "Invalid event: " + evtData;
                }
            }

            if (state.joined) {
                state.locNodeInfo.lastProcEvt = evtData.eventId();

                state.procEvtCnt++;

                if (state.procEvtCnt % evtsAckThreshold == 0)
                    updateNodeInfo = true;
            }
        }

        if (state.crd)
            handleProcessedEvents("procEvt", null);
        else if (updateNodeInfo) {
            assert state.locNodeZkPath != null;

            if (log.isInfoEnabled())
                log.info("Update processed events: " + state.locNodeInfo.lastProcEvt);

            state.zkClient.setData(state.locNodeZkPath, marshal(state.locNodeInfo), -1);
        }
    }

    /**
     * @param evtsData Events data.
     * @param evtData Local join event data.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processLocalJoin(ZkDiscoveryEventsData evtsData, final ZkDiscoveryNodeJoinEventData evtData)
        throws Exception
    {
        if (log.isInfoEnabled())
            log.info("Local join event data: " + evtData + ']');

        String path = zkPaths.joinEventDataPathForJoined(evtData.eventId());

        ZkJoinEventDataForJoined dataForJoined = unmarshalZip(state.zkClient.getData(path));

        state.gridStartTime = evtsData.gridStartTime;

        locNode.internalId(evtData.joinedInternalId);
        locNode.order(evtData.topologyVersion());

        DiscoveryDataBag dataBag = new DiscoveryDataBag(locNode.id());

        dataBag.commonData(dataForJoined.discoveryData());

        exchange.onExchange(dataBag);

        List<ZookeeperClusterNode> allNodes = dataForJoined.topology();

        for (int i = 0; i < allNodes.size(); i++) {
            ZookeeperClusterNode node = allNodes.get(i);

            node.setMetrics(new ClusterMetricsSnapshot());

            state.top.addNode(node);
        }

        state.top.addNode(locNode);

        final List<ClusterNode> topSnapshot = state.top.topologySnapshot();

        if (connState == ConnectionState.DISCONNECTED)
            connState = ConnectionState.STARTED;

        lsnr.onDiscovery(evtData.eventType(),
            evtData.topologyVersion(),
            locNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            null);

        if (state.prevJoined) {
            lsnr.onDiscovery(EVT_CLIENT_NODE_RECONNECTED,
                evtData.topologyVersion(),
                locNode,
                topSnapshot,
                Collections.<Long, Collection<ClusterNode>>emptyMap(),
                null);

            U.quietAndWarn(log, "Client node was reconnected after it was already considered failed.");
        }

        joinFut.onDone();

        state.joined = true;

        if (log.isDebugEnabled())
            log.debug("Delete data for joined: " + path);

        state.zkClient.deleteIfExistsAsync(path);
    }

    /**
     * @param evtData Event data.
     * @param msg Custom message.
     */
    @SuppressWarnings("unchecked")
    private void notifyCustomEvent(final ZkDiscoveryCustomEventData evtData, final DiscoverySpiCustomMessage msg) {
        if (log.isDebugEnabled())
            log.debug(" [topVer=" + evtData.topologyVersion() + ", msg=" + msg + ']');

        final ZookeeperClusterNode sndNode = state.top.nodesById.get(evtData.sndNodeId);

        assert sndNode != null : evtData;

        final List<ClusterNode> topSnapshot = state.top.topologySnapshot();

        lsnr.onDiscovery(evtData.eventType(),
            evtData.topologyVersion(),
            sndNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            msg);
    }

    /**
     * @param evtData Event data.
     * @param joiningData Joining node data.
     */
    @SuppressWarnings("unchecked")
    private void notifyNodeJoin(final ZkDiscoveryNodeJoinEventData evtData, ZkJoiningNodeData joiningData) {
        final ZookeeperClusterNode joinedNode = joiningData.node();

        joinedNode.order(evtData.topologyVersion());
        joinedNode.internalId(evtData.joinedInternalId);

        joinedNode.setMetrics(new ClusterMetricsSnapshot());

        state.top.addNode(joinedNode);

        final List<ClusterNode> topSnapshot = state.top.topologySnapshot();

        lsnr.onDiscovery(evtData.eventType(),
            evtData.topologyVersion(),
            joinedNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            null);
    }

    /**
     * @param evtData Event data.
     */
    @SuppressWarnings("unchecked")
    private void notifyNodeFail(final ZkDiscoveryNodeFailEventData evtData) {
        final ZookeeperClusterNode failedNode = state.top.removeNode(evtData.failedNodeInternalId());

        assert failedNode != null;

        final List<ClusterNode> topSnapshot = state.top.topologySnapshot();

        lsnr.onDiscovery(evtData.eventType(),
            evtData.topologyVersion(),
            failedNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            null);
    }

    /**
     * @param ctx Context for logging.
     * @param alives Optional alives nodes for additional filtering.
     * @throws Exception If failed.
     */
    private void handleProcessedEvents(String ctx, @Nullable TreeMap<Integer, String> alives) throws Exception {
        Iterator<ZkDiscoveryEventData> it = state.evtsData.evts.values().iterator();

        List<ZkDiscoveryCustomEventData> newEvts = null;

        ZkDiscoveryEventData prevEvtData = null;

        while (it.hasNext()) {
            ZkDiscoveryEventData evtData = it.next();

            if (evtData.allAcksReceived()) {
                if (prevEvtData != null) {
                    if (log.isInfoEnabled()) {
                        log.info("Previous event is not acked [" +
                            "evtId=" + evtData.eventId() +
                            ", prevEvtData=" + prevEvtData +
                            ", remaining=" + prevEvtData.remainingAcks() + ']');
                    }
                }

                prevEvtData = null;

                switch (evtData.eventType()) {
                    case EventType.EVT_NODE_JOINED: {
                        handleProcessedJoinEvent((ZkDiscoveryNodeJoinEventData)evtData);

                        break;
                    }

                    case DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT: {
                        DiscoverySpiCustomMessage ack = handleProcessedCustomEvent(ctx, (ZkDiscoveryCustomEventData)evtData);

                        if (ack != null) {
                            state.evtsData.evtIdGen++;

                            long evtId = state.evtsData.evtIdGen;

                            byte[] ackBytes = U.zip(marshal(ack));

                            String path = zkPaths.ackEventDataPath(evtId);

                            if (log.isDebugEnabled())
                                log.debug("Create ack event: " + path);

                            // TODO ZK: delete is previous exists?
                            state.zkClient.createIfNeeded(
                                path,
                                ackBytes,
                                CreateMode.PERSISTENT);

                            ZkDiscoveryCustomEventData ackEvtData = new ZkDiscoveryCustomEventData(
                                evtId,
                                evtData.topologyVersion(), // Use topology version from original event.
                                locNode.id(),
                                null,
                                true);

                            ackEvtData.msg = ack;

                            if (newEvts == null)
                                newEvts = new ArrayList<>();

                            newEvts.add(ackEvtData);

                            if (log.isDebugEnabled()) {
                                log.debug("Generated CUSTOM event ack [baseEvtId=" + evtData.eventId() +
                                    ", evt=" + ackEvtData +
                                    ", evtSize=" + ackBytes.length +
                                    ", msg=" + ack + ']');
                            }
                        }

                        break;
                    }

                    case EventType.EVT_NODE_FAILED: {
                        if (log.isDebugEnabled())
                            log.debug("All nodes processed node fail [evtData=" + evtData + ']');

                        break; // Do not need addition cleanup.
                    }
                }

                it.remove();
            }
            else
                prevEvtData = evtData;
        }

        if (newEvts != null) {
            Collection<ZookeeperClusterNode> nodes = state.top.nodesByOrder.values();

            for (int i = 0; i < newEvts.size(); i++)
                state.evtsData.addEvent(nodes, newEvts.get(i), alives);

            saveAndProcessNewEvents();
        }
    }

    /**
     * @param failedNode Failed node.
     * @throws Exception If failed.
     */
    private void handleProcessedEventsOnNodeFail(ZookeeperClusterNode failedNode, TreeMap<Integer, String> alives) throws Exception {
        boolean processed = false;

        for (Iterator<Map.Entry<Long, ZkDiscoveryEventData>> it = state.evtsData.evts.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, ZkDiscoveryEventData> e = it.next();

            ZkDiscoveryEventData evtData = e.getValue();

            if (evtData.onNodeFail(failedNode))
                processed = true;
        }

        if (processed)
            handleProcessedEvents("fail-" + failedNode.id(), alives);
    }

    /**
     * @param evtData Event data.
     * @throws Exception If failed.
     */
    private void handleProcessedJoinEvent(ZkDiscoveryNodeJoinEventData evtData) throws Exception {
        if (log.isInfoEnabled())
            log.info("All nodes processed node join [evtData=" + evtData + ']');

        String evtDataPath = zkPaths.joinEventDataPath(evtData.eventId());
        String dataForJoinedPath = zkPaths.joinEventDataPathForJoined(evtData.eventId());

        if (log.isInfoEnabled())
            log.info("Delete processed event data [path1=" + evtDataPath + ", path2=" + dataForJoinedPath + ']');

        state.zkClient.deleteIfExistsAsync(evtDataPath);
        state.zkClient.deleteIfExistsAsync(dataForJoinedPath);
    }

    /**
     * @param evtData Event data.
     * @throws Exception If failed.
     * @return Ack message.
     */
    @Nullable private DiscoverySpiCustomMessage handleProcessedCustomEvent(String ctx, ZkDiscoveryCustomEventData evtData)
        throws Exception
    {
        if (log.isDebugEnabled())
            log.debug("All nodes processed custom event [ctx=" + ctx + ", evtData=" + evtData + ']');

        if (!evtData.ackEvent()) {
            String path = zkPaths.customEventDataPath(false, evtData.evtPath);

            if (log.isDebugEnabled())
                log.debug("Delete path: " + path);

            state.zkClient.deleteIfExistsAsync(path);

            assert evtData.msg != null || locNode.order() > evtData.topologyVersion() : evtData;

            if (evtData.msg != null)
                return evtData.msg.ackMessage();
        }
        else {
            String path = zkPaths.ackEventDataPath(evtData.eventId());

            if (log.isDebugEnabled())
                log.debug("Delete path: " + path);

            state.zkClient.deleteIfExistsAsync(path);
        }

        return null;
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    public void stop() throws InterruptedException {
        stop0(new IgniteSpiException("Node stopped"));
    }

    /**
     * @param e Error.
     * @throws InterruptedException If interrupted.
     */
    private void stop0(Throwable e) throws InterruptedException {
        if (!stop.compareAndSet(false, true))
            return;

        log.info("Stop ZookeeperDiscovery [nodeId=" + locNode.id() + ", err=" + e + ']');

        synchronized (stateMux) {
            connState = ConnectionState.STOPPED;
        }

        ZookeeperClient zkClient = state.zkClient;

        if (zkClient != null)
            zkClient.onCloseStart();

        busyLock.block();

        busyLock.unblock();

        joinFut.onDone(e);

        if (zkClient != null)
            zkClient.close();
    }

    /**
     * @param busyLock Busy lock.
     * @param err Error.
     */
    private void onFatalError(GridSpinBusyLock busyLock, Throwable err) {
        busyLock.leaveBusy();

        if (err instanceof ZookeeperClientFailedException)
            return;

        if (connState == ConnectionState.STOPPED)
            return;

        // TODO ZK
        U.error(log, "Fatal error in ZookeeperDiscovery.", err);

        try {
            stop0(err);
        }
        catch (InterruptedException e) {
            U.warn(log, "Failed to finish stop procedure, thread was interrupted.");

            Thread.currentThread().interrupt();
        }

        if (err instanceof Error)
            throw (Error)err;
    }

    /**
     * @param bytes Bytes.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If failed.
     */
    private <T> T unmarshal(byte[] bytes) throws IgniteCheckedException {
        assert bytes != null && bytes.length > 0;

        return marsh.unmarshal(bytes, null);
    }

    private <T> T unmarshalZip(byte[] bytes) throws IgniteCheckedException {
        assert bytes != null && bytes.length > 0;

        return U.unmarshalZip(marsh, bytes, null);
    }

    /**
     * @param obj Object.
     * @return Bytes.
     * @throws IgniteCheckedException If failed.
     */
    private byte[] marshal(Object obj) throws IgniteCheckedException {
        assert obj != null;

        return marsh.marshal(obj);
    }

    /**
     *
     */
    private class ReconnectorThread extends IgniteSpiThread {
        /**
         *
         */
        ReconnectorThread() {
            super(ZookeeperDiscoveryImpl.this.igniteInstanceName, "zk-reconnector", log);
        }

        @Override protected void body() throws InterruptedException {
            busyLock.block();

            busyLock.unblock();

            UUID newId = UUID.randomUUID();

            U.quietAndWarn(log, "Connection to Zookeeper server is lost, local node will try to reconnect with new id [" +
                "newId=" + newId +
                ", prevId=" + locNode.id() +
                ", locNode=" + locNode + ']');

            doReconnect(newId);
        }
    }

    /**
     *
     */
    private class ConnectionLossListener implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void run() {
            if (clientReconnectEnabled) {
                synchronized (stateMux) {
                    if (connState == ConnectionState.STARTED)
                        connState = ConnectionState.DISCONNECTED;
                    else
                        return;
                }

                new ReconnectorThread().start();
            }
            else {
                U.warn(log, "Connection to Zookeeper server is lost, local node SEGMENTED.");

                onSegmented(new IgniteSpiException("Zookeeper connection loss."));
            }
        }
    }

    /**
     *
     */
    private class ZkWatcher implements Watcher {
        /** {@inheritDoc} */
        @Override public void process(WatchedEvent evt) {
            if (!busyLock.enterBusy())
                return;

            try {
                if (evt.getType() == Event.EventType.NodeDataChanged) {
                    if (evt.getPath().equals(zkPaths.evtsPath)) {
                        if (!state.crd)
                            state.zkClient.getDataAsync(evt.getPath(), this, dataCallback);
                    }
                    else
                        U.warn(log, "Received NodeDataChanged for unexpected path: " + evt.getPath());
                }
                else if (evt.getType() == Event.EventType.NodeChildrenChanged) {
                    if (evt.getPath().equals(zkPaths.aliveNodesDir))
                        state.zkClient.getChildrenAsync(evt.getPath(), this, childrenCallback);
                    else if (evt.getPath().equals(zkPaths.customEvtsDir))
                        state.zkClient.getChildrenAsync(evt.getPath(), this, childrenCallback);
                    else
                        U.warn(log, "Received NodeChildrenChanged for unexpected path: " + evt.getPath());
                }

                busyLock.leaveBusy();
            }
            catch (Throwable e) {
                onFatalError(busyLock, e);
            }
        }
    }

    /**
     *
     */
    private class ZKChildrenCallback implements AsyncCallback.Children2Callback {
        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            if (!busyLock.enterBusy())
                return;

            try {
                assert rc == 0 : KeeperException.Code.get(rc);

                if (path.equals(zkPaths.aliveNodesDir))
                    generateTopologyEvents(children);
                else if (path.equals(zkPaths.customEvtsDir))
                    generateCustomEvents(children);
                else
                    U.warn(log, "Children callback for unexpected path: " + path);

                busyLock.leaveBusy();
            }
            catch (Throwable e) {
                onFatalError(busyLock, e);
            }
        }
    }

    /**
     *
     */
    private class ZkDataCallback implements AsyncCallback.DataCallback {
        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (!busyLock.enterBusy())
                return;

            try {
                assert rc == 0 : KeeperException.Code.get(rc);

                if (path.equals(zkPaths.evtsPath)) {
                    if (!state.crd)
                        processNewEvents(data);
                }
                else
                    U.warn(log, "Data callback for unknown path: " + path);

                busyLock.leaveBusy();
            }
            catch (Throwable e) {
                onFatalError(busyLock, e);
            }
        }
    }

    /**
     *
     */
    private class AliveNodeDataWatcher implements Watcher, AsyncCallback.DataCallback {
        /** {@inheritDoc} */
        @Override public void process(WatchedEvent evt) {
            if (!busyLock.enterBusy())
                return;

            try {
                if (evt.getType() == Event.EventType.NodeDataChanged)
                    state.zkClient.getDataAsync(evt.getPath(), this, this);

                busyLock.leaveBusy();
            }
            catch (Throwable e) {
                onFatalError(busyLock, e);
            }
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (!busyLock.enterBusy())
                return;

            try {
                assert state.crd;

                processResult0(rc, path, data);

                busyLock.leaveBusy();
            }
            catch (Throwable e) {
                onFatalError(busyLock, e);
            }
        }

        /**
         * @param rc Result code.
         * @param path Path.
         * @param data Data.
         * @throws Exception If failed.
         */
        private void processResult0(int rc, String path, byte[] data) throws Exception {
            if (rc == KeeperException.Code.NONODE.intValue()) {
                if (log.isDebugEnabled())
                    log.debug("Alive node callaback, no node: " + path);

                return;
            }

            assert rc == 0 : KeeperException.Code.get(rc);

            if (data.length > 0) {
                ZkAliveNodeData nodeData = unmarshal(data);

                Integer nodeInternalId = ZkIgnitePaths.aliveInternalId(path);

                Iterator<ZkDiscoveryEventData> it = state.evtsData.evts.values().iterator();

                boolean processed = false;

                while (it.hasNext()) {
                    ZkDiscoveryEventData evtData = it.next();

                    if (evtData.onAckReceived(nodeInternalId, nodeData.lastProcEvt))
                        processed = true;
                }

                if (processed)
                    handleProcessedEvents("ack-" + nodeInternalId, null);
            }
        }
    }

    /**
     *
     */
    private class PreviousNodeWatcher implements Watcher, AsyncCallback.StatCallback {
        /** {@inheritDoc} */
        @Override public void process(WatchedEvent evt) {
            if (!busyLock.enterBusy())
                return;

            try {
                if (evt.getType() == Event.EventType.NodeDeleted) {
                    onPreviousNodeFail();
                }
                else {
                    if (log.isInfoEnabled())
                        log.info("Previous node watch event: " + evt);

                    if (evt.getType() != Event.EventType.None)
                        state.zkClient.existsAsync(evt.getPath(), this, this);
                }

                busyLock.leaveBusy();
            }
            catch (Throwable e) {
                onFatalError(busyLock, e);
            }
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, Stat stat) {
            if (!busyLock.enterBusy())
                return;

            try {
                log.info("Previous node stat callback [rc=" + rc + ", path=" + path + ", stat=" + stat + ']');

                assert rc == 0 || rc == KeeperException.Code.NONODE.intValue() : KeeperException.Code.get(rc);

                if (rc == KeeperException.Code.NONODE.intValue() || stat == null)
                    onPreviousNodeFail();

                busyLock.leaveBusy();
            }
            catch (Throwable e) {
                onFatalError(busyLock, e);
            }
        }
    }

    /**
     *
     */
    class CheckCoordinatorCallback implements  AsyncCallback.Children2Callback {
        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            if (!busyLock.enterBusy())
                return;

            try {
                assert rc == 0 : rc;

                checkIsCoordinator(rc, children);

                busyLock.leaveBusy();
            }
            catch (Throwable e) {
                onFatalError(busyLock, e);
            }
        }
    }

    /**
     *
     */
    enum ConnectionState {
        /** */
        STARTED,
        /** */
        DISCONNECTED,
        /** */
        STOPPED
    }
}
