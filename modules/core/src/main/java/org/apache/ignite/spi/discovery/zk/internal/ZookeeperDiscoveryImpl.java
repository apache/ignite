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
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;

/**
 * TODO ZK: check if compression makes sense.
 */
public class ZookeeperDiscoveryImpl {
    /** */
    static final String IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD = "IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD";

    /** */
    private final ZookeeperDiscoverySpi spi;

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
    private IgniteThreadPoolExecutor utilityPool;

    /** */
    private ZkRuntimeState rtState;

    /** */
    private volatile ConnectionState connState = ConnectionState.STARTED;

    /** */
    private final AtomicBoolean stop = new AtomicBoolean();

    /** */
    private final Object stateMux = new Object();

    /** */
    public volatile IgniteDiscoverySpiInternalListener internalLsnr;

    /**
     * @param log Logger.
     * @param zkRootPath Zookeeper base path node all nodes.
     * @param locNode Local node instance.
     * @param lsnr Discovery events listener.
     * @param exchange Discovery data exchange.
     */
    public ZookeeperDiscoveryImpl(
        ZookeeperDiscoverySpi spi,
        String igniteInstanceName,
        IgniteLogger log,
        String zkRootPath,
        ZookeeperClusterNode locNode,
        DiscoverySpiListener lsnr,
        DiscoverySpiDataExchange exchange) {
        assert locNode.id() != null && locNode.isLocal() : locNode;

        MarshallerUtils.setNodeName(marsh, igniteInstanceName);

        ZkIgnitePaths.validatePath(zkRootPath);

        zkPaths = new ZkIgnitePaths(zkRootPath);

        this.spi = spi;
        this.igniteInstanceName = igniteInstanceName;
        this.connectString = spi.getZkConnectionString();
        this.sesTimeout = spi.getSessionTimeout();
        this.log = log.getLogger(getClass());
        this.locNode = locNode;
        this.lsnr = lsnr;
        this.exchange = exchange;
        this.clientReconnectEnabled = locNode.isClient() && !spi.isClientReconnectDisabled();

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

        return rtState.top.nodesById.get(nodeId);
    }

    /**
     * @param nodeId Node ID.
     * @return Ping result.
     */
    public boolean pingNode(UUID nodeId) {
        // TODO ZK
        if (connState == ConnectionState.DISCONNECTED)
            throw new IgniteClientDisconnectedException(null, "Client is disconnected.");

        return node(nodeId) != null;
    }

    /**
     * @param nodeId Node ID.
     * @param warning Warning.
     */
    public void failNode(UUID nodeId, @Nullable String warning) {
        ZookeeperClusterNode node = rtState.top.nodesById.get(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignore forcible node fail request, node does not exist: " + nodeId);

            return;
        }

        if (!node.isClient()) {
            U.warn(log, "Ignore forcible node fail request for non-client node: " + node);

            return;
        }

        sendCustomMessage(new ZkInternalForceNodeFailMessage(nodeId, warning));
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

        rtState.zkClient.onCloseStart();

        busyLock.block();

        busyLock.unblock();

        rtState.zkClient.close();

        UUID newId = UUID.randomUUID();

        U.quietAndWarn(log, "Local node will try to reconnect to cluster with new id due to network problems [" +
            "newId=" + newId +
            ", prevId=" + locNode.id() +
            ", locNode=" + locNode + ']');

        runInWorkerThread(new ReconnectClosure(newId));
    }

    /**
     * @param newId New ID.
     */
    private void doReconnect(UUID newId) {
        if (rtState.joined) {
            assert rtState.evtsData != null;

            lsnr.onDiscovery(EVT_CLIENT_NODE_DISCONNECTED,
                rtState.evtsData.topVer,
                locNode,
                rtState.top.topologySnapshot(),
                Collections.<Long, Collection<ClusterNode>>emptyMap(),
                null);
        }

        try {
            locNode.onClientDisconnected(newId);

            joinTopology0(rtState.joined);
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
        if (rtState.joined) {
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
            notifySegmented();
        }
    }

    /**
     *
     */
    private void notifySegmented() {
        assert rtState.evtsData != null;

        lsnr.onDiscovery(EventType.EVT_NODE_SEGMENTED,
            rtState.evtsData.topVer,
            locNode,
            rtState.top.topologySnapshot(),
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            null);
    }

    /**
     * @return Remote nodes.
     */
    public Collection<ClusterNode> remoteNodes() {
        checkState();

        return rtState.top.remoteNodes();
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
            List<String> children = rtState.zkClient.getChildren(zkPaths.aliveNodesDir);

            for (int i = 0; i < children.size(); i++) {
                UUID id = ZkIgnitePaths.aliveNodeId(children.get(i));

                if (nodeId.equals(id))
                    return true;
            }

            return false;
        }
        catch (ZookeeperClientFailedException e) {
            if (clientReconnectEnabled)
                throw new IgniteClientDisconnectedException(null, "Client is disconnected.");

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
            msgBytes = marshalZip(msg);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal custom message: " + msg, e);
        }

        while (!busyLock.enterBusy())
            checkState();

        try {
            ZookeeperClient zkClient = rtState.zkClient;

            String prefix = UUID.randomUUID().toString();

            int partCnt = 1;

            int overhead = 10;

            UUID locId = locNode.id();

            String path = zkPaths.createCustomEventPath(prefix, locId, partCnt);

            if (zkClient.needSplitNodeData(path, msgBytes, overhead)) {
                List<byte[]> parts = zkClient.splitNodeData(path, msgBytes, overhead);

                String partsBasePath = zkPaths.customEventPartsBasePath(prefix, locId);

                saveMultipleParts(zkClient, partsBasePath, parts);

                msgBytes = null;

                partCnt = parts.size();
            }

            zkClient.createSequential(prefix,
                zkPaths.customEvtsDir,
                zkPaths.createCustomEventPath(prefix, locId, partCnt),
                msgBytes,
                CreateMode.PERSISTENT_SEQUENTIAL);
        }
        catch (ZookeeperClientFailedException e) {
            if (clientReconnectEnabled)
                throw new IgniteClientDisconnectedException(null, "Client is disconnected.");

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
        return rtState.gridStartTime;
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
     * @param prevJoined {@code True} if reconnect after already joined topology
     *    in this case (need produce EVT_CLIENT_NODE_RECONNECTED event).
     * @throws InterruptedException If interrupted.
     */
    private void joinTopology0(boolean prevJoined) throws InterruptedException {
        IgniteDiscoverySpiInternalListener internalLsnr = this.internalLsnr;

        if (internalLsnr != null)
            internalLsnr.beforeJoin(log);

        rtState = new ZkRuntimeState(prevJoined);

        DiscoveryDataBag discoDataBag = new DiscoveryDataBag(locNode.id());

        exchange.collect(discoDataBag);

        ZkJoiningNodeData joinData = new ZkJoiningNodeData(locNode, discoDataBag.joiningNodeData());

        byte[] joinDataBytes;

        try {
            joinDataBytes = marshalZip(joinData);
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to marshal joining node data", e);
        }

        try {
            rtState.zkClient = new ZookeeperClient(igniteInstanceName,
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
            if (rtState.zkClient.exists(zkPaths.aliveNodesDir))
                return; // This path is created last, assume all others dirs are created.

            List<String> dirs = new ArrayList<>();

            // TODO ZK: test create all parents?

            dirs.add(zkPaths.clusterDir);
            dirs.add(zkPaths.evtsPath);
            dirs.add(zkPaths.joinDataDir);
            dirs.add(zkPaths.customEvtsDir);
            dirs.add(zkPaths.customEvtsPartsDir);
            dirs.add(zkPaths.customEvtsAcksDir);
            dirs.add(zkPaths.aliveNodesDir);

            try {
                rtState.zkClient.createAll(dirs, PERSISTENT);
            }
            catch (KeeperException.NodeExistsException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to create nodes using bulk operation: " + e);

                for (String dir : dirs)
                    rtState.zkClient.createIfNeeded(dir, null, PERSISTENT);
            }
        }
        catch (ZookeeperClientFailedException e) {
            throw new IgniteSpiException("Failed to initialize Zookeeper nodes", e);
        }
    }

    private void deleteMultiplePartsAsync(ZookeeperClient zkClient, String basePath, int partCnt) {
        for (int i = 0; i < partCnt; i++) {
            String path = multipartPathName(basePath, i);

            zkClient.deleteIfExistsAsync(path);
        }

    }

    private byte[] readMultipleParts(ZookeeperClient zkClient, String basePath, int partCnt)
        throws Exception {
        assert partCnt >= 1;

        if (partCnt > 1) {
            List<byte[]> parts = new ArrayList<>(partCnt);

            int totSize = 0;

            for (int i = 0; i < partCnt; i++) {
                byte[] part = zkClient.getData(multipartPathName(basePath, i));

                parts.add(part);

                totSize += part.length;
            }

            byte[] res = new byte[totSize];

            int pos = 0;

            for (int i = 0; i < partCnt; i++) {
                byte[] part = parts.get(i);

                System.arraycopy(part, 0, res, pos, part.length);

                pos += part.length;
            }

            return res;
        }
        else
            return zkClient.getData(multipartPathName(basePath, 0));
    }

    private int saveMultipleParts(ZookeeperClient zkClient, String basePath, List<byte[]> parts)
        throws ZookeeperClientFailedException, InterruptedException
    {
        assert parts.size() > 1;

        for (int i = 0; i < parts.size(); i++) {
            byte[] part = parts.get(i);

            String path = multipartPathName(basePath, i);

            zkClient.createIfNeeded(path, part, PERSISTENT);
        }

        return parts.size();
    }

    private static String multipartPathName(String basePath, int part) {
        return basePath + String.format("%04d", part);
    }

    /**
     * @param joinDataBytes Joining node data.
     * @throws InterruptedException If interrupted.
     */
    private void startJoin(final byte[] joinDataBytes) throws InterruptedException {
        if (!busyLock.enterBusy())
            return;

        try {
            long startTime = System.currentTimeMillis();

            initZkNodes();

            String prefix = UUID.randomUUID().toString();

            final ZkRuntimeState rtState = this.rtState;

            ZookeeperClient zkClient = rtState.zkClient;

            final int OVERHEAD = 5;

            // TODO ZK: need clean up join data if failed before was able to create alive node.
            String joinDataPath = zkPaths.joinDataDir + "/" + prefix + ":" + locNode.id();

            if (zkClient.needSplitNodeData(joinDataPath, joinDataBytes, OVERHEAD)) {
                List<byte[]> parts = zkClient.splitNodeData(joinDataPath, joinDataBytes, OVERHEAD);

                rtState.joinDataPartCnt = parts.size();

                saveMultipleParts(zkClient, joinDataPath + ":", parts);

                joinDataPath = zkClient.createIfNeeded(
                    joinDataPath,
                    marshalZip(new ZkJoiningNodeData(parts.size())),
                    PERSISTENT);
            }
            else {
                joinDataPath = zkClient.createIfNeeded(
                    joinDataPath,
                    joinDataBytes,
                    PERSISTENT);
            }

            rtState.locNodeZkPath = zkClient.createSequential(
                prefix,
                zkPaths.aliveNodesDir,
                zkPaths.aliveNodesDir + "/" + prefix + ":" + locNode.id() + "|",
                null,
                EPHEMERAL_SEQUENTIAL);

            log.info("Node started join [nodeId=" + locNode.id() +
                ", instanceName=" + locNode.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME) +
                ", joinDataSize=" + joinDataBytes.length +
                ", joinDataPartCnt=" + rtState.joinDataPartCnt +
                ", consistentId=" + locNode.consistentId() +
                ", initTime=" + (System.currentTimeMillis() - startTime) +
                ", nodePath=" + rtState.locNodeZkPath + ']');

            rtState.internalOrder = ZkIgnitePaths.aliveInternalId(rtState.locNodeZkPath);

            /*
            If node can not join due to validation error this error is reported in join data,
            As a minor optimization do not start watch join data immediately, but only if do not receive
            join event after some timeout.
             */
            rtState.joinTimeoutObj = new CheckJoinStateTimeoutObject(
                joinDataPath,
                rtState);

            spi.getSpiContext().addTimeoutObject(rtState.joinTimeoutObj);

            zkClient.getChildrenAsync(zkPaths.aliveNodesDir, null, new CheckCoordinatorCallback());

            zkClient.getDataAsync(zkPaths.evtsPath, watcher, dataCallback);
        }
        catch (IgniteCheckedException | ZookeeperClientFailedException e) {
            throw new IgniteSpiException("Failed to initialize Zookeeper nodes", e);
        }
        finally {
            busyLock.leaveBusy();

            connStartLatch.countDown();
        }
    }

    /**
     *
     */
    private class CheckJoinStateTimeoutObject implements IgniteSpiTimeoutObject, Watcher, AsyncCallback.DataCallback {
        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private final long endTime = System.currentTimeMillis() + 5000;

        /** */
        private final String joinDataPath;

        /** */
        private final ZkRuntimeState rtState;

        /**
         * @param joinDataPath Node joined data path.
         * @param rtState State.
         */
        CheckJoinStateTimeoutObject(String joinDataPath, ZkRuntimeState rtState) {
            this.joinDataPath = joinDataPath;
            this.rtState = rtState;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (rtState.joined)
                return;

            synchronized (stateMux) {
                if (connState != ConnectionState.STARTED)
                    return;
            }

            rtState.zkClient.getDataAsync(joinDataPath, this, this);
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (rc != 0)
                return;

            if (!busyLock.enterBusy())
                return;

            try {
                Object obj = unmarshalZip(data);

                if (obj instanceof ZkInternalJoinErrorMessage) {
                    ZkInternalJoinErrorMessage joinErr = (ZkInternalJoinErrorMessage)obj;

                    onSegmented(new IgniteSpiException(joinErr.err));
                }

                busyLock.leaveBusy();
            }
            catch (Throwable e) {
                onFatalError(busyLock, e);
            }
        }

        /** {@inheritDoc} */
        @Override public void process(WatchedEvent evt) {
            if (!busyLock.enterBusy())
                return;

            try {
                if (evt.getType() == Event.EventType.NodeDataChanged)
                    rtState.zkClient.getDataAsync(evt.getPath(), this, this);

                busyLock.leaveBusy();
            }
            catch (Throwable e) {
                onFatalError(busyLock, e);
            }
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

        int locInternalId = ZkIgnitePaths.aliveInternalId(rtState.locNodeZkPath);

        for (String aliveNodePath : aliveNodes) {
            Integer internalId = ZkIgnitePaths.aliveInternalId(aliveNodePath);

            alives.put(internalId, aliveNodePath);
        }

        assert !alives.isEmpty();

        Map.Entry<Integer, String> crdE = alives.firstEntry();

        if (locInternalId == crdE.getKey())
            onBecomeCoordinator(aliveNodes, locInternalId);
        else {
            assert alives.size() > 1;

            Map.Entry<Integer, String> prevE = alives.floorEntry(locInternalId - 1);

            assert prevE != null;

            log.info("Discovery coordinator already exists, watch for previous node [" +
                "locId=" + locNode.id() +
                ", prevPath=" + prevE.getValue() + ']');

            PreviousNodeWatcher watcher = new PreviousNodeWatcher();

            rtState.zkClient.existsAsync(zkPaths.aliveNodesDir + "/" + prevE.getValue(), watcher, watcher);
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

        rtState.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, null, new CheckCoordinatorCallback());
    }

    /**
     * @param aliveNodes Alive nodes paths.
     * @param locInternalId Local node's internal ID.
     * @throws Exception If failed.
     */
    private void onBecomeCoordinator(List<String> aliveNodes, int locInternalId) throws Exception {
        byte[] evtsDataBytes = rtState.zkClient.getData(zkPaths.evtsPath);

        if (evtsDataBytes.length > 0)
            processNewEvents(evtsDataBytes);

        rtState.crd = true;

        if (rtState.joined) {
            if (log.isInfoEnabled())
                log.info("Node is new discovery coordinator [locId=" + locNode.id() + ']');

            assert locNode.order() > 0 : locNode;
            assert rtState.evtsData != null;

            for (ZkDiscoveryEventData evtData : rtState.evtsData.evts.values())
                evtData.initRemainingAcks(rtState.top.nodesByOrder.values());

            handleProcessedEvents("crd");
        }
        else {
            if (log.isInfoEnabled())
                log.info("Node is first cluster node [locId=" + locNode.id() + ']');

            newClusterStarted(locInternalId);
        }

        rtState.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, watcher, childrenCallback);
        rtState.zkClient.getChildrenAsync(zkPaths.customEvtsDir, watcher, childrenCallback);

        for (String alivePath : aliveNodes)
            watchAliveNodeData(alivePath);
    }

    /**
     * @param alivePath
     */
    private void watchAliveNodeData(String alivePath) {
        assert rtState.locNodeZkPath != null;

        String path = zkPaths.aliveNodesDir + "/" + alivePath;

        if (!path.equals(rtState.locNodeZkPath))
            rtState.zkClient.getDataAsync(path, aliveNodeDataWatcher, aliveNodeDataWatcher);
    }

    /**
     * @param aliveNodes ZK nodes representing alive cluster nodes.
     * @throws Exception If failed.
     */
    private void generateTopologyEvents(List<String> aliveNodes) throws Exception {
        assert rtState.crd;

        if (log.isInfoEnabled())
            log.info("Process alive nodes change [alives=" + aliveNodes.size() + "]");

        TreeMap<Integer, String> alives = new TreeMap<>();

        TreeMap<Long, ZookeeperClusterNode> curTop = new TreeMap<>(rtState.top.nodesByOrder);

        boolean newEvts = false;

        for (String child : aliveNodes) {
            Integer internalId = ZkIgnitePaths.aliveInternalId(child);

            Object old = alives.put(internalId, child);

            assert old == null;

            if (!rtState.top.nodesByInternalId.containsKey(internalId)) {
                if (processJoinOnCoordinator(curTop, internalId, child))
                    newEvts = true;
            }
        }

        List<ZookeeperClusterNode> failedNodes = null;

        for (Map.Entry<Integer, ZookeeperClusterNode> e : rtState.top.nodesByInternalId.entrySet()) {
            if (!alives.containsKey(e.getKey())) {
                ZookeeperClusterNode failedNode = e.getValue();

                if (failedNodes == null)
                    failedNodes = new ArrayList<>();

                failedNodes.add(failedNode);

                generateNodeFail(curTop, failedNode);

                newEvts = true;
            }
        }

        if (newEvts)
            saveAndProcessNewEvents();

        if (failedNodes != null)
            handleProcessedEventsOnNodesFail(failedNodes);
    }

    private ZkJoiningNodeData unmarshalJoinData(UUID nodeId, UUID prefixId) throws Exception {
        String joinDataPath = zkPaths.joiningNodeDataPath(nodeId, prefixId);

        byte[] joinData = rtState.zkClient.getData(joinDataPath);

        Object dataObj = unmarshalZip(joinData);

        if (!(dataObj instanceof ZkJoiningNodeData))
            throw new Exception("Invalid joined node data: " + dataObj);

        ZkJoiningNodeData joiningNodeData = (ZkJoiningNodeData)dataObj;

        if (joiningNodeData.partCount() > 1) {
            joinData = readMultipleParts(rtState.zkClient, joinDataPath + ":", joiningNodeData.partCount());

            joiningNodeData = unmarshalZip(joinData);
        }

        return joiningNodeData;
    }

    /**
     * @param nodeId
     * @param aliveNodePath
     * @return
     * @throws Exception If failed.
     */
    private Object unmarshalJoinDataOnCoordinator(UUID nodeId, UUID prefixId, String aliveNodePath) throws Exception {
        String joinDataPath = zkPaths.joiningNodeDataPath(nodeId, prefixId);

        byte[] joinData = rtState.zkClient.getData(joinDataPath);

        Object dataObj;

        try {
            dataObj = unmarshalZip(joinData);

            if (dataObj instanceof ZkInternalJoinErrorMessage)
                return dataObj;
        }
        catch (Exception e) {
            U.error(log, "Failed to unmarshal joining node data [nodePath=" + aliveNodePath + "']", e);

            return new ZkInternalJoinErrorMessage(ZkIgnitePaths.aliveInternalId(aliveNodePath),
                "Failed to unmarshal join data: " + e);
        }

        assert dataObj instanceof ZkJoiningNodeData : dataObj;

        ZkJoiningNodeData joiningNodeData = (ZkJoiningNodeData)dataObj;

        if (joiningNodeData.partCount() > 1) {
            joinData = readMultipleParts(rtState.zkClient, joinDataPath + ":", joiningNodeData.partCount());

            try {
                joiningNodeData = unmarshalZip(joinData);
            }
            catch (Exception e) {
                U.error(log, "Failed to unmarshal joining node data [nodePath=" + aliveNodePath + "']", e);

                return new ZkInternalJoinErrorMessage(ZkIgnitePaths.aliveInternalId(aliveNodePath),
                    "Failed to unmarshal join data: " + e);
            }
        }

        assert joiningNodeData.node() != null : joiningNodeData;

        return joiningNodeData;
    }

    /**
     * @param curTop Current nodes.
     * @param internalId Joined node internal ID.
     * @param aliveNodePath Joined node path.
     * @throws Exception If failed.
     */
    private boolean processJoinOnCoordinator(
        TreeMap<Long, ZookeeperClusterNode> curTop,
        int internalId,
        String aliveNodePath)
        throws Exception
    {
        UUID nodeId = ZkIgnitePaths.aliveNodeId(aliveNodePath);
        UUID prefixId = ZkIgnitePaths.aliveNodePrefixId(aliveNodePath);

        Object data = unmarshalJoinDataOnCoordinator(nodeId, prefixId, aliveNodePath);

        ZkInternalJoinErrorMessage joinErr = null;
        ZkJoiningNodeData joiningNodeData = null;

        if (data instanceof ZkJoiningNodeData) {
            joiningNodeData = (ZkJoiningNodeData)data;

            String err = validateJoiningNode(joiningNodeData.node());

            if (err != null)
                joinErr = new ZkInternalJoinErrorMessage(ZkIgnitePaths.aliveInternalId(aliveNodePath), err);
        }
        else {
            assert data instanceof ZkInternalJoinErrorMessage : data;

            joinErr = (ZkInternalJoinErrorMessage)data;
        }

        if (joinErr == null) {
            ZookeeperClusterNode joinedNode = joiningNodeData.node();

            assert nodeId.equals(joinedNode.id()) : joiningNodeData.node();

            generateNodeJoin(curTop, joiningNodeData, internalId, prefixId);

            watchAliveNodeData(aliveNodePath);

            return true;
        }
        else {
            if (joinErr.notifyNode) {
                String joinDataPath = zkPaths.joiningNodeDataPath(nodeId, prefixId);

                zkClient().setData(joinDataPath, marshalZip(joinErr), -1);

                zkClient().deleteIfExists(zkPaths.aliveNodesDir + "/" + aliveNodePath, -1);
            }
            else {
                if (log.isInfoEnabled())
                    log.info("Ignore join data, node was failed by previous coordinator: " + aliveNodePath);

                zkClient().deleteIfExists(zkPaths.aliveNodesDir + "/" + aliveNodePath, -1);
            }

            return false;
        }
    }

    /**
     * @param node Joining node.
     * @return Non null error message if validation failed.
     */
    @Nullable private String validateJoiningNode(ZookeeperClusterNode node) {
        ZookeeperClusterNode node0 = rtState.top.nodesById.get(node.id());

        if (node0 != null) {
            U.error(log, "Failed to include node in cluster, node with the same ID already exists [joiningNode=" + node +
                ", existingNode=" + node0 + ']');

            return "Node with the same ID already exists: " + node0;
        }

        IgniteNodeValidationResult err = spi.getSpiContext().validateNode(node);

        if (err != null) {
            LT.warn(log, err.message());

            return err.sendMessage();
        }

        return null;
    }

    /**
     * @throws Exception If failed.
     */
    private void saveAndProcessNewEvents() throws Exception {
        long start = System.currentTimeMillis();

        byte[] evtsBytes = marshalZip(rtState.evtsData);

        rtState.zkClient.setData(zkPaths.evtsPath, evtsBytes, -1);

        long time = System.currentTimeMillis() - start;

        if (log.isInfoEnabled()) {
            log.info("Discovery coordinator saved new topology events [topVer=" + rtState.evtsData.topVer +
                ", size=" + evtsBytes.length +
                ", evts=" + rtState.evtsData.evts.size() +
                ", lastEvt=" + rtState.evtsData.evtIdGen +
                ", saveTime=" + time + ']');
        }

        processNewEvents(rtState.evtsData);
    }

    /**
     * @param curTop Current topology.
     * @param failedNode Failed node.
     */
    private void generateNodeFail(TreeMap<Long, ZookeeperClusterNode> curTop, ZookeeperClusterNode failedNode) {
        Object rmvd = curTop.remove(failedNode.order());

        assert rmvd != null;

        rtState.evtsData.topVer++;
        rtState.evtsData.evtIdGen++;

        ZkDiscoveryNodeFailEventData evtData = new ZkDiscoveryNodeFailEventData(
            rtState.evtsData.evtIdGen,
            rtState.evtsData.topVer,
            failedNode.internalId());

        rtState.evtsData.addEvent(curTop.values(), evtData);

        if (log.isInfoEnabled())
            log.info("Generated NODE_FAILED event [evt=" + evtData + ']');
    }

    /**
     * @param curTop Current nodes.
     * @param internalId Joined node internal ID.
     * @throws Exception If failed.
     */
    private void generateNodeJoin(
        TreeMap<Long, ZookeeperClusterNode> curTop,
        ZkJoiningNodeData joiningNodeData,
        int internalId,
        UUID prefixId)
        throws Exception
    {
        ZookeeperClusterNode joinedNode = joiningNodeData.node();

        UUID nodeId = joinedNode.id();

        rtState.evtsData.topVer++;
        rtState.evtsData.evtIdGen++;

        joinedNode.order(rtState.evtsData.topVer);
        joinedNode.internalId(internalId);

        long evtId = rtState.evtsData.evtIdGen;

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

        long addDataStart = System.currentTimeMillis();

        byte[] dataForJoinedBytes = marshalZip(dataForJoined);

        int overhead = 5;

        String dataPathForJoined = zkPaths.joinEventDataPathForJoined(evtId);

        int dataForJoinedPartCnt = 1;

        if (rtState.zkClient.needSplitNodeData(dataPathForJoined, dataForJoinedBytes, overhead)) {
            dataForJoinedPartCnt = saveMultipleParts(rtState.zkClient,
                dataPathForJoined,
                rtState.zkClient.splitNodeData(dataPathForJoined, dataForJoinedBytes, overhead));
        }
        else {
            rtState.zkClient.createIfNeeded(multipartPathName(dataPathForJoined, 0),
                dataForJoinedBytes,
                PERSISTENT);
        }

        long addDataTime = System.currentTimeMillis() - addDataStart;

        ZkDiscoveryNodeJoinEventData evtData = new ZkDiscoveryNodeJoinEventData(
            evtId,
            rtState.evtsData.topVer,
            joinedNode.id(),
            joinedNode.internalId(),
            prefixId,
            joiningNodeData.partCount(),
            dataForJoinedPartCnt);

        evtData.joiningNodeData = joiningNodeData;

        rtState.evtsData.addEvent(dataForJoined.topology(), evtData);

        evtData.addRemainingAck(joinedNode); // Topology for joined node does not contain joined node.

        if (log.isInfoEnabled()) {
            log.info("Generated NODE_JOINED event [evt=" + evtData +
                ", dataForJoinedSize=" + dataForJoinedBytes.length +
                ", dataForJoinedPartCnt=" + dataForJoinedPartCnt +
                ", addDataTime=" + addDataTime + ']');
        }
    }

    /**
     * @param locInternalId Local node internal ID.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void newClusterStarted(int locInternalId) throws Exception {
        spi.getSpiContext().removeTimeoutObject(rtState.joinTimeoutObj);

        cleanupPreviousClusterData();

        rtState.joined = true;

        rtState.gridStartTime = U.currentTimeMillis();

        rtState.evtsData = new ZkDiscoveryEventsData(rtState.gridStartTime, 1L, new TreeMap<Long, ZkDiscoveryEventData>());

        locNode.internalId(locInternalId);
        locNode.order(1);

        rtState.top.addNode(locNode);

        String locAlivePath = rtState.locNodeZkPath.substring(rtState.locNodeZkPath.lastIndexOf('/') + 1);

        deleteJoiningNodeData(locNode.id(),
            ZkIgnitePaths.aliveNodePrefixId(locAlivePath),
            rtState.joinDataPartCnt);

        final List<ClusterNode> topSnapshot = Collections.singletonList((ClusterNode)locNode);

        if (connState == ConnectionState.DISCONNECTED)
            connState = ConnectionState.STARTED;

        lsnr.onDiscovery(EventType.EVT_NODE_JOINED,
            1L,
            locNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            null);

        if (rtState.prevJoined) {
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
        long start = System.currentTimeMillis();

        // TODO ZK: use multi, better batching.
        rtState.zkClient.setData(zkPaths.evtsPath, null, -1);

        List<String> evtChildren = rtState.zkClient.getChildren(zkPaths.evtsPath);

        for (String evtPath : evtChildren) {
            String evtDir = zkPaths.evtsPath + "/" + evtPath;

            removeChildren(evtDir);
        }

        rtState.zkClient.deleteAll(zkPaths.evtsPath, evtChildren, -1);

        rtState.zkClient.deleteAll(zkPaths.customEvtsDir,
            rtState.zkClient.getChildren(zkPaths.customEvtsDir),
            -1);

        rtState.zkClient.deleteAll(zkPaths.customEvtsPartsDir,
            rtState.zkClient.getChildren(zkPaths.customEvtsPartsDir),
            -1);

        rtState.zkClient.deleteAll(zkPaths.customEvtsAcksDir,
            rtState.zkClient.getChildren(zkPaths.customEvtsAcksDir),
            -1);

        long time = System.currentTimeMillis() - start;

        if (time > 0) {
            if (log.isInfoEnabled())
                log.info("Previous cluster data cleanup time: " + time);
        }
    }

    /**
     * @param path Path.
     * @throws Exception If failed.
     */
    private void removeChildren(String path) throws Exception {
        rtState.zkClient.deleteAll(path, rtState.zkClient.getChildren(path), -1);
    }

    ZkClusterNodes nodes() {
        return rtState.top;
    }

    ZookeeperClient zkClient() {
        return rtState.zkClient;
    }

    private byte[] readCustomEventData(String evtPath, UUID sndNodeId) throws Exception {
        int partCnt = ZkIgnitePaths.customEventPartsCount(evtPath);

        if (partCnt > 1) {
            String partsBasePath = zkPaths.customEventPartsBasePath(
                ZkIgnitePaths.customEventPrefix(evtPath), sndNodeId);

            return readMultipleParts(rtState.zkClient, partsBasePath, partCnt);
        }
        else
            return rtState.zkClient.getData(zkPaths.customEvtsDir + "/" + evtPath);
    }

    /**
     * @param customEvtNodes ZK nodes representing custom events to process.
     * @throws Exception If failed.
     */
    private void generateCustomEvents(List<String> customEvtNodes) throws Exception {
        assert rtState.crd;

        TreeMap<Integer, String> newEvts = null;

        for (int i = 0; i < customEvtNodes.size(); i++) {
            String evtPath = customEvtNodes.get(i);

            int evtSeq = ZkIgnitePaths.customEventSequence(evtPath);

            if (evtSeq > rtState.evtsData.procCustEvt) {
                if (newEvts == null)
                    newEvts = new TreeMap<>();

                newEvts.put(evtSeq, evtPath);
            }
        }

        if (newEvts != null) {
            Set<UUID> alives = null;

            for (Map.Entry<Integer, String> evtE : newEvts.entrySet()) {
                String evtPath = evtE.getValue();

                UUID sndNodeId = ZkIgnitePaths.customEventSendNodeId(evtPath);

                ZookeeperClusterNode sndNode = rtState.top.nodesById.get(sndNodeId);

                if (alives != null && !alives.contains(sndNode.id()))
                    sndNode = null;

                if (sndNode != null) {
                    byte[] evtBytes = readCustomEventData(evtPath, sndNodeId);

                    DiscoverySpiCustomMessage msg;

                    try {
                        msg = unmarshalZip(evtBytes);

                        rtState.evtsData.evtIdGen++;

                        if (msg instanceof ZkInternalForceNodeFailMessage) {
                            ZkInternalForceNodeFailMessage msg0 = (ZkInternalForceNodeFailMessage)msg;

                            if (alives == null)
                                alives = new HashSet<>(rtState.top.nodesById.keySet());

                            if (alives.contains(msg0.nodeId)) {
                                rtState.evtsData.topVer++;

                                alives.remove(msg0.nodeId);

                                ZookeeperClusterNode node = rtState.top.nodesById.get(msg0.nodeId);

                                assert node != null :  msg0.nodeId;

                                for (String child : zkClient().getChildren(zkPaths.aliveNodesDir)) {
                                    if (ZkIgnitePaths.aliveInternalId(child) == node.internalId()) {
                                        zkClient().deleteIfExistsAsync(zkPaths.aliveNodesDir + "/" + child);

                                        break;
                                    }
                                }
                            }
                            else {
                                if (log.isDebugEnabled())
                                    log.debug("Ignore forcible node fail request for unknown node: " + msg0.nodeId);
                            }
                        }

                        ZkDiscoveryCustomEventData evtData = new ZkDiscoveryCustomEventData(
                            rtState.evtsData.evtIdGen,
                            rtState.evtsData.topVer,
                            sndNodeId,
                            evtPath,
                            false);

                        evtData.msg = msg;

                        rtState.evtsData.addEvent(rtState.top.nodesByOrder.values(), evtData);

                        if (log.isDebugEnabled())
                            log.debug("Generated CUSTOM event [evt=" + evtData + ", msg=" + msg + ']');
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to unmarshal custom discovery message: " + e, e);

                        deleteCustomEventData(rtState.zkClient, evtPath);
                    }
                }
                else {
                    U.warn(log, "Ignore custom event from unknown node: " + sndNodeId);

                    deleteCustomEventData(rtState.zkClient, evtPath);
                }

                rtState.evtsData.procCustEvt = evtE.getKey();
            }

            saveAndProcessNewEvents();
        }
    }

    private void deleteCustomEventData(ZookeeperClient zkClient, String evtPath) {
        if (log.isDebugEnabled())
            log.debug("Delete custom event data: " + evtPath);

        String prefix = ZkIgnitePaths.customEventPrefix(evtPath);
        UUID sndNodeId = ZkIgnitePaths.customEventSendNodeId(evtPath);
        int partCnt = ZkIgnitePaths.customEventPartsCount(evtPath);

        assert partCnt >= 1 : partCnt;

        if (partCnt > 1) {
            for (int i = 0; i < partCnt; i++) {
                String path0 = zkPaths.customEventPartPath(prefix, sndNodeId, i);

                zkClient.deleteIfExistsAsync(path0);
            }
        }

        zkClient.deleteIfExistsAsync(zkPaths.customEvtsDir + "/" + evtPath);
    }

    /**
     * @param data Marshalled events.
     * @throws Exception If failed.
     */
    private void processNewEvents(byte[] data) throws Exception {
        if (data.length == 0)
            return;

        assert !rtState.crd;

        ZkDiscoveryEventsData newEvts = unmarshalZip(data);

        // Need keep processed custom events since they contains message object.
        if (rtState.evtsData != null) {
            for (Map.Entry<Long, ZkDiscoveryEventData> e : rtState.evtsData.evts.entrySet()) {
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

        rtState.evtsData = newEvts;
    }

    /**
     * @param evtsData Events.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processNewEvents(final ZkDiscoveryEventsData evtsData) throws Exception {
        TreeMap<Long, ZkDiscoveryEventData> evts = evtsData.evts;

        boolean updateNodeInfo = false;

        for (ZkDiscoveryEventData evtData : evts.tailMap(rtState.locNodeInfo.lastProcEvt, false).values()) {
            if (!rtState.joined) {
                if (evtData.eventType() != EventType.EVT_NODE_JOINED)
                    continue;

                ZkDiscoveryNodeJoinEventData evtData0 = (ZkDiscoveryNodeJoinEventData)evtData;

                UUID joinedId = evtData0.nodeId;

                boolean locJoin = evtData.eventType() == EventType.EVT_NODE_JOINED &&
                    evtData0.joinedInternalId == rtState.internalOrder;

                if (locJoin) {
                    assert locNode.id().equals(joinedId);

                    processLocalJoin(evtsData, evtData0);
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("New discovery event data [evt=" + evtData + ", evtsHist=" + evts.size() + ']');

                switch (evtData.eventType()) {
                    case EventType.EVT_NODE_JOINED: {
                        ZkDiscoveryNodeJoinEventData evtData0 = (ZkDiscoveryNodeJoinEventData)evtData;

                        ZkJoiningNodeData joiningData;

                        if (rtState.crd) {
                            assert evtData0.joiningNodeData != null;

                            joiningData = evtData0.joiningNodeData;
                        }
                        else {
                            joiningData = unmarshalJoinData(evtData0.nodeId, evtData0.joinDataPrefixId);

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

                        if (rtState.crd) {
                            assert evtData0.msg != null : evtData0;

                            msg = evtData0.msg;
                        }
                        else {
                            if (evtData0.ackEvent()) {
                                String path = zkPaths.ackEventDataPath(evtData0.eventId());

                                msg = unmarshalZip(rtState.zkClient.getData(path));
                            }
                            else {
                                byte[] msgBytes = readCustomEventData(evtData0.evtPath, evtData0.sndNodeId);

                                msg = unmarshalZip(msgBytes);
                            }

                            evtData0.msg = msg;
                        }

                        if (msg instanceof ZkInternalMessage)
                            processInternalMessage(evtData0, (ZkInternalMessage)msg);
                        else {
                            notifyCustomEvent(evtData0, msg);

                            if (!evtData0.ackEvent())
                                updateNodeInfo = true;
                        }

                        break;
                    }

                    default:
                        assert false : "Invalid event: " + evtData;
                }
            }

            if (rtState.joined) {
                rtState.locNodeInfo.lastProcEvt = evtData.eventId();

                rtState.procEvtCnt++;

                if (rtState.procEvtCnt % evtsAckThreshold == 0)
                    updateNodeInfo = true;
            }
        }

        if (rtState.crd)
            handleProcessedEvents("procEvt");
        else if (updateNodeInfo) {
            assert rtState.locNodeZkPath != null;

            if (log.isDebugEnabled())
                log.debug("Update processed events: " + rtState.locNodeInfo.lastProcEvt);

            rtState.zkClient.setData(rtState.locNodeZkPath, marshalZip(rtState.locNodeInfo), -1);
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

        spi.getSpiContext().removeTimeoutObject(rtState.joinTimeoutObj);

        String path = zkPaths.joinEventDataPathForJoined(evtData.eventId());

        byte[] dataForJoinedBytes = readMultipleParts(rtState.zkClient, path, evtData.dataForJoinedPartCnt);

        ZkJoinEventDataForJoined dataForJoined = unmarshalZip(dataForJoinedBytes);

        rtState.gridStartTime = evtsData.gridStartTime;

        locNode.internalId(evtData.joinedInternalId);
        locNode.order(evtData.topologyVersion());

        DiscoveryDataBag dataBag = new DiscoveryDataBag(locNode.id());

        dataBag.commonData(dataForJoined.discoveryData());

        exchange.onExchange(dataBag);

        List<ZookeeperClusterNode> allNodes = dataForJoined.topology();

        for (int i = 0; i < allNodes.size(); i++) {
            ZookeeperClusterNode node = allNodes.get(i);

            node.setMetrics(new ClusterMetricsSnapshot());

            rtState.top.addNode(node);
        }

        rtState.top.addNode(locNode);

        final List<ClusterNode> topSnapshot = rtState.top.topologySnapshot();

        if (connState == ConnectionState.DISCONNECTED)
            connState = ConnectionState.STARTED;

        lsnr.onDiscovery(evtData.eventType(),
            evtData.topologyVersion(),
            locNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            null);

        if (rtState.prevJoined) {
            lsnr.onDiscovery(EVT_CLIENT_NODE_RECONNECTED,
                evtData.topologyVersion(),
                locNode,
                topSnapshot,
                Collections.<Long, Collection<ClusterNode>>emptyMap(),
                null);

            U.quietAndWarn(log, "Client node was reconnected after it was already considered failed.");
        }

        joinFut.onDone();

        rtState.joined = true;

        deleteDataForJoined(evtData);
    }

    /**
     * @param evtData
     * @param msg
     */
    private void processInternalMessage(ZkDiscoveryCustomEventData evtData, ZkInternalMessage msg) throws Exception {
        if (msg instanceof ZkInternalForceNodeFailMessage) {
            ZkInternalForceNodeFailMessage msg0 = (ZkInternalForceNodeFailMessage)msg;

            ClusterNode creatorNode = rtState.top.nodesById.get(evtData.sndNodeId);

            if (msg0.warning != null) {
                U.warn(log, "Received EVT_NODE_FAILED event with warning [" +
                    "nodeInitiatedEvt=" + (creatorNode != null ? creatorNode : evtData.sndNodeId) +
                    ", nodeId=" + msg0.nodeId +
                    ", msg=" + msg0.warning + ']');
            }
            else {
                U.warn(log, "Received force EVT_NODE_FAILED event [" +
                    "nodeInitiatedEvt=" + (creatorNode != null ? creatorNode : evtData.sndNodeId) +
                    ", nodeId=" + msg0.nodeId + ']');
            }

            ZookeeperClusterNode node = rtState.top.nodesById.get(msg0.nodeId);

            assert node != null : msg0.nodeId;

            processNodeFail(node.internalId(), evtData.topologyVersion());
        }
    }

    /**
     *
     */
    public void simulateNodeFailure() {
        zkClient().deleteIfExistsAsync(zkPaths.aliveNodesDir);

        zkClient().onCloseStart();

        zkClient().close();
    }

    /**
     * @param evtData Event data.
     * @param msg Custom message.
     */
    @SuppressWarnings("unchecked")
    private void notifyCustomEvent(final ZkDiscoveryCustomEventData evtData, final DiscoverySpiCustomMessage msg) {
        if (log.isDebugEnabled())
            log.debug(" [topVer=" + evtData.topologyVersion() + ", msg=" + msg + ']');

        final ZookeeperClusterNode sndNode = rtState.top.nodesById.get(evtData.sndNodeId);

        assert sndNode != null : evtData;

        final List<ClusterNode> topSnapshot = rtState.top.topologySnapshot();

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

        rtState.top.addNode(joinedNode);

        final List<ClusterNode> topSnapshot = rtState.top.topologySnapshot();

        lsnr.onDiscovery(evtData.eventType(),
            evtData.topologyVersion(),
            joinedNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            null);
    }

    /**
     * @param evtData Event data.
     * @throws Exception If failed.
     */
    private void notifyNodeFail(final ZkDiscoveryNodeFailEventData evtData) throws Exception {
        processNodeFail(evtData.failedNodeInternalId(), evtData.topologyVersion());
    }

    /**
     * @param nodeInternalId Failed node internal ID.
     * @param topVer Topology version.
     * @throws Exception If failed.
     */
    private void processNodeFail(int nodeInternalId, long topVer) throws Exception {
        final ZookeeperClusterNode failedNode = rtState.top.removeNode(nodeInternalId);

        assert failedNode != null;

        if (failedNode.isLocal()) {
            U.warn(log, "Received EVT_NODE_FAILED for local node.");

            zkClient().onCloseStart();

            if (locNode.isClient() && clientReconnectEnabled) {
                boolean reconnect = false;

                synchronized (stateMux) {
                    if (connState == ConnectionState.STARTED) {
                        reconnect = true;

                        connState = ConnectionState.DISCONNECTED;
                    }
                }

                if (reconnect) {
                    UUID newId = UUID.randomUUID();

                    U.quietAndWarn(log, "Received EVT_NODE_FAILED for local node, will try to reconnect with new id [" +
                        "newId=" + newId +
                        ", prevId=" + locNode.id() +
                        ", locNode=" + locNode + ']');

                    runInWorkerThread(new ReconnectClosure(newId));
                }
            }
            else
                notifySegmented();

            // Stop any further processing.
            throw new ZookeeperClientFailedException("Received node failed event for local node.");
        }
        else {
            final List<ClusterNode> topSnapshot = rtState.top.topologySnapshot();

            lsnr.onDiscovery(EVT_NODE_FAILED,
                topVer,
                failedNode,
                topSnapshot,
                Collections.<Long, Collection<ClusterNode>>emptyMap(),
                null);
        }
    }

    /**
     * @param ctx Context for logging.
     * @throws Exception If failed.
     */
    private void handleProcessedEvents(String ctx) throws Exception {
        Iterator<ZkDiscoveryEventData> it = rtState.evtsData.evts.values().iterator();

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
                            rtState.evtsData.evtIdGen++;

                            long evtId = rtState.evtsData.evtIdGen;

                            byte[] ackBytes = marshalZip(ack);

                            String path = zkPaths.ackEventDataPath(evtId);

                            if (log.isDebugEnabled())
                                log.debug("Create ack event: " + path);

                            // TODO ZK: delete is previous exists?
                            rtState.zkClient.createIfNeeded(
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
            Collection<ZookeeperClusterNode> nodes = rtState.top.nodesByOrder.values();

            for (int i = 0; i < newEvts.size(); i++)
                rtState.evtsData.addEvent(nodes, newEvts.get(i));

            saveAndProcessNewEvents();
        }
    }

    /**
     * @param failedNodes Failed nodes.
     * @throws Exception If failed.
     */
    private void handleProcessedEventsOnNodesFail(List<ZookeeperClusterNode> failedNodes) throws Exception {
        boolean processed = false;

        for (Iterator<Map.Entry<Long, ZkDiscoveryEventData>> it = rtState.evtsData.evts.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, ZkDiscoveryEventData> e = it.next();

            ZkDiscoveryEventData evtData = e.getValue();

            for (int i = 0; i < failedNodes.size(); i++) {
                ZookeeperClusterNode failedNode = failedNodes.get(i);

                if (evtData.onNodeFail(failedNode))
                    processed = true;
            }
        }

        if (processed)
            handleProcessedEvents("fail-" + U.nodeIds(failedNodes));
    }

    /**
     * @param evtData Event data.
     * @throws Exception If failed.
     */
    private void handleProcessedJoinEvent(ZkDiscoveryNodeJoinEventData evtData) throws Exception {
        if (log.isDebugEnabled())
            log.debug("All nodes processed node join [evtData=" + evtData + ']');

        deleteJoiningNodeData(evtData.nodeId, evtData.joinDataPrefixId, evtData.joinDataPartCnt);

        deleteDataForJoined(evtData);
    }

    private void deleteJoiningNodeData(UUID nodeId, UUID joinDataPrefixId, int partCnt) throws Exception {
        String evtDataPath = zkPaths.joiningNodeDataPath(nodeId, joinDataPrefixId);

        if (log.isDebugEnabled())
            log.debug("Delete joining node data [path=" + evtDataPath + ']');

        rtState.zkClient.deleteIfExistsAsync(evtDataPath);

        if (partCnt > 1)
            deleteMultiplePartsAsync(rtState.zkClient, evtDataPath + ":", partCnt);
    }

    private void deleteDataForJoined(ZkDiscoveryNodeJoinEventData evtData) {
        String dataForJoinedPath = zkPaths.joinEventDataPathForJoined(evtData.eventId());

        if (log.isDebugEnabled())
            log.debug("Delete data for joined node [path=" + dataForJoinedPath + ']');

        deleteMultiplePartsAsync(rtState.zkClient, dataForJoinedPath, evtData.dataForJoinedPartCnt);
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
            deleteCustomEventData(rtState.zkClient, evtData.evtPath);

            assert evtData.msg != null || locNode.order() > evtData.topologyVersion() : evtData;

            if (evtData.msg != null)
                return evtData.msg.ackMessage();
        }
        else {
            String path = zkPaths.ackEventDataPath(evtData.eventId());

            if (log.isDebugEnabled())
                log.debug("Delete path: " + path);

            rtState.zkClient.deleteIfExistsAsync(path);
        }

        return null;
    }

    /**
     * @param c Closure to run.
     */
    private void runInWorkerThread(Runnable c) {
        IgniteThreadPoolExecutor pool;

        synchronized (stateMux) {
            if (connState == ConnectionState.STOPPED)
                return;

            if (utilityPool == null) {
                utilityPool = new IgniteThreadPoolExecutor("zk-discovery-pool",
                    igniteInstanceName,
                    0,
                    1,
                    2000,
                    new LinkedBlockingQueue<Runnable>());
            }

            pool = utilityPool;
        }

        pool.submit(c);
    }

    /**
     *
     */
    public void onStop() {
        if (!stop.compareAndSet(false, true))
            return;

        synchronized (stateMux) {
            connState = ConnectionState.STOPPED;
        }

        ZookeeperClient zkClient = rtState.zkClient;

        if (zkClient != null)
            zkClient.onCloseStart();
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
        busyLock.block();

        busyLock.unblock();

        joinFut.onDone(e);

        IgniteUtils.shutdownNow(ZookeeperDiscoveryImpl.class, utilityPool, log);

        ZookeeperClient zkClient = rtState.zkClient;

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
            onStop();

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
    private <T> T unmarshalZip(byte[] bytes) throws IgniteCheckedException {
        assert bytes != null && bytes.length > 0;

        return U.unmarshalZip(marsh, bytes, null);
    }

    /**
     * @param obj Object.
     * @return Bytes.
     * @throws IgniteCheckedException If failed.
     */
    private byte[] marshalZip(Object obj) throws IgniteCheckedException {
        assert obj != null;

        return U.zip(marsh.marshal(obj));
    }

    /**
     *
     */
    private class ReconnectClosure implements Runnable {
        /** */
        private final UUID newId;

        /**
         * @param newId New ID.
         */
        ReconnectClosure(UUID newId) {
            assert newId != null;

            this.newId = newId;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            busyLock.block();

            busyLock.unblock();

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

                UUID newId = UUID.randomUUID();

                U.quietAndWarn(log, "Connection to Zookeeper server is lost, local node will try to reconnect with new id [" +
                    "newId=" + newId +
                    ", prevId=" + locNode.id() +
                    ", locNode=" + locNode + ']');

                runInWorkerThread(new ReconnectClosure(newId));
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
                        if (!rtState.crd)
                            rtState.zkClient.getDataAsync(evt.getPath(), this, dataCallback);
                    }
                    else
                        U.warn(log, "Received NodeDataChanged for unexpected path: " + evt.getPath());
                }
                else if (evt.getType() == Event.EventType.NodeChildrenChanged) {
                    if (evt.getPath().equals(zkPaths.aliveNodesDir))
                        rtState.zkClient.getChildrenAsync(evt.getPath(), this, childrenCallback);
                    else if (evt.getPath().equals(zkPaths.customEvtsDir))
                        rtState.zkClient.getChildrenAsync(evt.getPath(), this, childrenCallback);
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
                    if (!rtState.crd)
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
                    rtState.zkClient.getDataAsync(evt.getPath(), this, this);

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
                assert rtState.crd;

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
                ZkAliveNodeData nodeData = unmarshalZip(data);

                Integer nodeInternalId = ZkIgnitePaths.aliveInternalId(path);

                Iterator<ZkDiscoveryEventData> it = rtState.evtsData.evts.values().iterator();

                boolean processed = false;

                while (it.hasNext()) {
                    ZkDiscoveryEventData evtData = it.next();

                    if (evtData.onAckReceived(nodeInternalId, nodeData.lastProcEvt))
                        processed = true;
                }

                if (processed)
                    handleProcessedEvents("ack-" + nodeInternalId);
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
                        rtState.zkClient.existsAsync(evt.getPath(), this, this);
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
