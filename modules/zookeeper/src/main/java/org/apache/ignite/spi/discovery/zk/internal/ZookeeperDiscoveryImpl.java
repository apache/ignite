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

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CommunicationFailureResolver;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.io.GridByteArrayOutputStream;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2;
import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;

/**
 * Zookeeper Discovery Impl.
 */
public class ZookeeperDiscoveryImpl {
    /** */
    static final String IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD = "IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD";

    /** */
    static final String IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_TIMEOUT = "IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_TIMEOUT";

    /** */
    static final String IGNITE_ZOOKEEPER_DISCOVERY_SPI_MAX_EVTS = "IGNITE_ZOOKEEPER_DISCOVERY_SPI_MAX_EVTS";

    /** */
    private static final String IGNITE_ZOOKEEPER_DISCOVERY_SPI_EVTS_THROTTLE = "IGNITE_ZOOKEEPER_DISCOVERY_SPI_EVTS_THROTTLE";

    /** */
    final ZookeeperDiscoverySpi spi;

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
    final GridSpinBusyLock busyLock = new GridSpinBusyLock();

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

    /** */
    private final ConcurrentHashMap<Long, PingFuture> pingFuts = new ConcurrentHashMap<>();

    /** */
    private final AtomicReference<ZkCommunicationErrorProcessFuture> commErrProcFut = new AtomicReference<>();

    /** */
    private long prevSavedEvtsTopVer;

    /** */
    private final ZookeeperDiscoveryStatistics stats;

    /**
     * @param spi Discovery SPI.
     * @param igniteInstanceName Instance name.
     * @param log Logger.
     * @param zkRootPath Zookeeper base path node all nodes.
     * @param locNode Local node instance.
     * @param lsnr Discovery events listener.
     * @param exchange Discovery data exchange.
     * @param internalLsnr Internal listener (used for testing only).
     * @param stats Zookeeper DiscoverySpi statistics collector.
     */
    public ZookeeperDiscoveryImpl(
        ZookeeperDiscoverySpi spi,
        String igniteInstanceName,
        IgniteLogger log,
        String zkRootPath,
        ZookeeperClusterNode locNode,
        DiscoverySpiListener lsnr,
        DiscoverySpiDataExchange exchange,
        IgniteDiscoverySpiInternalListener internalLsnr,
        ZookeeperDiscoveryStatistics stats) {
        assert locNode.id() != null && locNode.isLocal() : locNode;

        MarshallerUtils.setNodeName(marsh, igniteInstanceName);

        zkPaths = new ZkIgnitePaths(zkRootPath);

        this.spi = spi;
        this.igniteInstanceName = igniteInstanceName;
        this.connectString = spi.getZkConnectionString();
        this.sesTimeout = (int)spi.getSessionTimeout();
        this.log = log.getLogger(getClass());
        this.locNode = locNode;
        this.lsnr = lsnr;
        this.exchange = exchange;
        this.clientReconnectEnabled = locNode.isClient() && !spi.isClientReconnectDisabled();

        int evtsAckThreshold = IgniteSystemProperties.getInteger(IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD, 5);

        if (evtsAckThreshold <= 0)
            evtsAckThreshold = 1;

        this.evtsAckThreshold = evtsAckThreshold;

        if (internalLsnr != null)
            this.internalLsnr = internalLsnr;

        this.stats = stats;
    }

    /**
     * @return Exception.
     */
    private static IgniteClientDisconnectedCheckedException disconnectError() {
        return new IgniteClientDisconnectedCheckedException(null, "Client node disconnected.");
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
    @Nullable public ZookeeperClusterNode node(UUID nodeId) {
        assert nodeId != null;

        return rtState.top.nodesById.get(nodeId);
    }

    /**
     * @param nodeOrder Node order.
     * @return Node instance.
     */
    @Nullable public ZookeeperClusterNode node(long nodeOrder) {
        assert nodeOrder > 0 : nodeOrder;

        return rtState.top.nodesByOrder.get(nodeOrder);
    }

    /**
     * @param fut Future to remove.
     */
    void clearCommunicationErrorProcessFuture(ZkCommunicationErrorProcessFuture fut) {
        assert fut.isDone() : fut;

        commErrProcFut.compareAndSet(fut, null);
    }

    /**
     * @param node0 Problem node ID
     * @param err Connect error.
     */
    public void resolveCommunicationError(ClusterNode node0, Exception err) {
        ZookeeperClusterNode node = node(node0.id());

        if (node == null)
            throw new IgniteSpiException(new ClusterTopologyCheckedException("Node failed: " + node0.id()));

        IgniteInternalFuture<Boolean> nodeStatusFut;

        for (;;) {
            checkState();

            ZkCommunicationErrorProcessFuture fut = commErrProcFut.get();

            if (fut == null || fut.isDone()) {
                ZkCommunicationErrorProcessFuture newFut = ZkCommunicationErrorProcessFuture.createOnCommunicationError(
                    this,
                    node.sessionTimeout() + 1000);

                if (commErrProcFut.compareAndSet(fut, newFut)) {
                    fut = newFut;

                    if (log.isInfoEnabled()) {
                        log.info("Created new communication error process future [errNode=" + node0.id() +
                            ", err=" + err + ']');
                    }

                    try {
                        checkState();
                    }
                    catch (Exception e) {
                        fut.onError(e);

                        throw e;
                    }

                    fut.scheduleCheckOnTimeout();
                }
                else {
                    fut = commErrProcFut.get();

                    if (fut == null)
                        continue;
                }
            }

            nodeStatusFut = fut.nodeStatusFuture(node);

            if (nodeStatusFut != null)
                break;
            else {
                try {
                    fut.get();
                }
                catch (IgniteCheckedException e) {
                    U.warn(log, "Previous communication error process future failed: " + e);
                }
            }
        }

        try {
            if (!nodeStatusFut.get())
                throw new IgniteSpiException(new ClusterTopologyCheckedException("Node failed: " + node0.id()));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException(e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @return Ping result.
     */
    public boolean pingNode(UUID nodeId) {
        checkState();

        ZkRuntimeState rtState = this.rtState;

        ZookeeperClusterNode node = rtState.top.nodesById.get(nodeId);

        if (node == null)
            return false;

        if (node.isLocal())
            return true;

        PingFuture fut = pingFuts.get(node.order());

        if (fut == null) {
            fut = new PingFuture(rtState, node);

            PingFuture old = pingFuts.putIfAbsent(node.order(), fut);

            if (old == null) {
                if (fut.checkNodeAndState())
                    spi.getSpiContext().addTimeoutObject(fut);
                else
                    assert fut.isDone();
            }
            else
                fut = old;
        }

        try {
            return fut.get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException(e);
        }
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

        sendCustomMessage(new ZkForceNodeFailMessage(node.internalId(), warning));
    }

    /**
     *
     */
    public void reconnect() {
        assert clientReconnectEnabled;

        synchronized (stateMux) {
            if (connState == ConnectionState.STARTED) {
                connState = ConnectionState.DISCONNECTED;

                rtState.onCloseStart(disconnectError());
            }
            else
                return;
        }

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

            joinTopology(rtState);
        }
        catch (Exception e) {
            if (stopping()) {
                if (log.isDebugEnabled())
                    log.debug("Reconnect failed, node is stopping [err=" + e + ']');

                return;
            }

            U.error(log, "Failed to reconnect: " + e, e);

            onSegmented(e);
        }
    }

    /**
     * @return {@code True} if started to stop.
     */
    private boolean stopping() {
        if (stop.get())
            return true;

        synchronized (stateMux) {
            if (connState == ConnectionState.STOPPED)
                return true;
        }

        return false;
    }

    /**
     * @param e Error.
     */
    private void onSegmented(Exception e) {
        rtState.errForClose = e;

        if (rtState.joined || joinFut.isDone()) {
            synchronized (stateMux) {
                connState = ConnectionState.STOPPED;
            }

            notifySegmented();
        }
        else
            joinFut.onDone(e);
    }

    /**
     *
     */
    private void notifySegmented() {
        List<ClusterNode> nodes = rtState.top.topologySnapshot();

        if (nodes.isEmpty())
            nodes = Collections.singletonList((ClusterNode)locNode);

        lsnr.onDiscovery(EVT_NODE_SEGMENTED,
            rtState.evtsData != null ? rtState.evtsData.topVer : 1L,
            locNode,
            nodes,
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
                throw new IgniteSpiException("Node stopped.");

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

            saveCustomMessage(zkClient, msgBytes);
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
     * @param zkClient Client.
     * @param msgBytes Marshalled message.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    private void saveCustomMessage(ZookeeperClient zkClient, byte[] msgBytes)
        throws ZookeeperClientFailedException, InterruptedException
    {
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

    /**
     * @return Cluster start time.
     */
    public long gridStartTime() {
        return rtState.gridStartTime;
    }

    /**
     * Starts join procedure and waits for {@link EventType#EVT_NODE_JOINED} event for local node.
     *
     * @throws InterruptedException If interrupted.
     */
    public void startJoinAndWait() throws InterruptedException {
        joinTopology(null);

        for (;;) {
            try {
                joinFut.get(10_000);

                break;
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                U.warn(log, "Waiting for local join event [nodeId=" + locNode.id() + ", name=" + igniteInstanceName + ']');
            }
            catch (Exception e) {
                IgniteSpiException spiErr = X.cause(e, IgniteSpiException.class);

                if (spiErr != null)
                    throw spiErr;

                throw new IgniteSpiException("Failed to join cluster", e);
            }
        }
    }

    /**
     * @param prevState Previous state in case of connect retry.
     * @throws InterruptedException If interrupted.
     */
    private void joinTopology(@Nullable ZkRuntimeState prevState) throws InterruptedException {
        if (!busyLock.enterBusy())
            return;

        try {
            boolean reconnect = prevState != null;

            // Need fire EVT_CLIENT_NODE_RECONNECTED event if reconnect after already joined.
            boolean prevJoined = prevState != null && prevState.joined;

            IgniteDiscoverySpiInternalListener internalLsnr = this.internalLsnr;

            if (internalLsnr != null)
                internalLsnr.beforeJoin(locNode, log);

            if (locNode.isClient() && reconnect)
                locNode.setAttributes(spi.getSpiContext().nodeAttributes());

            marshalCredentialsOnJoin(locNode);

            synchronized (stateMux) {
                if (connState == ConnectionState.STOPPED)
                    return;

                connState = ConnectionState.STARTED;
            }

            ZkRuntimeState rtState = this.rtState = new ZkRuntimeState(prevJoined);

            DiscoveryDataBag discoDataBag = new DiscoveryDataBag(locNode.id(), locNode.isClient());

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
                rtState.zkClient = new ZookeeperClient(
                    igniteInstanceName,
                    log,
                    connectString,
                    sesTimeout,
                    new ConnectionLossListener());
            }
            catch (Exception e) {
                throw new IgniteSpiException("Failed to create Zookeeper client", e);
            }

            startJoin(rtState, prevState, joinDataBytes);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    private void initZkNodes() throws InterruptedException {
        try {
            ZookeeperClient client = rtState.zkClient;

            if (!client.exists(zkPaths.clusterDir)) {
                createRootPathParents(zkPaths.clusterDir, client);

                client.createIfNeeded(zkPaths.clusterDir, null, PERSISTENT);
            }

            List<String> createdDirs = client.getChildren(zkPaths.clusterDir);

            String[] requiredDirs = {
                zkPaths.evtsPath,
                zkPaths.joinDataDir,
                zkPaths.customEvtsDir,
                zkPaths.customEvtsPartsDir,
                zkPaths.customEvtsAcksDir,
                zkPaths.aliveNodesDir};

            List<String> dirs = new ArrayList<>();

            for (String dir : requiredDirs) {
                String dir0 = dir.substring(zkPaths.clusterDir.length() + 1);

                if (!createdDirs.contains(dir0))
                    dirs.add(dir);
            }

            if (!dirs.isEmpty())
                client.createAll(dirs, PERSISTENT);
        }
        catch (ZookeeperClientFailedException e) {
            throw new IgniteSpiException("Failed to initialize Zookeeper nodes", e);
        }
    }

    /**
     * @param rootDir Root directory.
     * @param client Client.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    private void createRootPathParents(String rootDir, ZookeeperClient client)
        throws ZookeeperClientFailedException, InterruptedException {
        int startIdx = 0;

        for (;;) {
            int separatorIdx = rootDir.indexOf(ZkIgnitePaths.PATH_SEPARATOR, startIdx);

            if (separatorIdx == -1)
                break;

            if (separatorIdx > 0) {
                String path = rootDir.substring(0, separatorIdx);

                client.createIfNeeded(path, null, CreateMode.PERSISTENT);
            }

            startIdx = separatorIdx + 1;
        }
    }

    /**
     * @param zkClient Client.
     * @param basePath Base path.
     * @param partCnt Parts count.
     */
    private void deleteMultiplePartsAsync(ZookeeperClient zkClient, String basePath, int partCnt) {
        for (int i = 0; i < partCnt; i++) {
            String path = multipartPathName(basePath, i);

            zkClient.deleteIfExistsAsync(path);
        }
    }

    /**
     * @param zkClient Client.
     * @param basePath Base path.
     * @param partCnt Parts count.
     * @return Read parts.
     * @throws Exception If failed.
     */
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

    /**
     * @param zkClient Client.
     * @param basePath Base path.
     * @param parts Data parts.
     * @return Number of parts.
     * @throws ZookeeperClientFailedException If client failed.
     * @throws InterruptedException If interrupted.
     */
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

    /**
     * @param basePath Base path.
     * @param part Part number.
     * @return Path.
     */
    private static String multipartPathName(String basePath, int part) {
        return basePath + String.format("%04d", part);
    }

    /**
     * @param rtState Runtime state.
     * @param joinDataBytes Joining node data.
     * @param prevState Previous state in case of connect retry.
     * @throws InterruptedException If interrupted.
     */
    private void startJoin(ZkRuntimeState rtState, @Nullable ZkRuntimeState prevState, final byte[] joinDataBytes)
        throws InterruptedException
    {
        try {
            long startTime = System.currentTimeMillis();

            initZkNodes();

            String prefix = UUID.randomUUID().toString();

            rtState.init(new ZkWatcher(rtState), new AliveNodeDataWatcher(rtState));

            ZookeeperClient zkClient = rtState.zkClient;

            final int OVERHEAD = 5;

            // TODO ZK: https://issues.apache.org/jira/browse/IGNITE-8193
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
                zkPaths.aliveNodePathForCreate(prefix, locNode),
                null,
                EPHEMERAL_SEQUENTIAL);

            rtState.internalOrder = ZkIgnitePaths.aliveInternalId(rtState.locNodeZkPath);

            if (log.isInfoEnabled()) {
                log.info("Node started join [nodeId=" + locNode.id() +
                    ", instanceName=" + locNode.attribute(ATTR_IGNITE_INSTANCE_NAME) +
                    ", zkSessionId=0x" + Long.toHexString(rtState.zkClient.zk().getSessionId()) +
                    ", joinDataSize=" + joinDataBytes.length +
                    (rtState.joinDataPartCnt > 1 ? (", joinDataPartCnt=" + rtState.joinDataPartCnt) : "") +
                    ", consistentId=" + locNode.consistentId() +
                    ", initTime=" + (System.currentTimeMillis() - startTime) +
                    ", nodePath=" + rtState.locNodeZkPath + ']');
            }

            /*
            If node can not join due to validation error this error is reported in join data,
            As a minor optimization do not start watch join data immediately, but only if do not receive
            join event after some timeout.
             */
            CheckJoinErrorWatcher joinErrorWatcher = new CheckJoinErrorWatcher(5000, joinDataPath, rtState);

            rtState.joinErrTo = joinErrorWatcher.timeoutObj;

            if (locNode.isClient() && spi.getJoinTimeout() > 0) {
                ZkTimeoutObject joinTimeoutObj = prevState != null ? prevState.joinTo : null;

                if (joinTimeoutObj == null) {
                    joinTimeoutObj = new JoinTimeoutObject(spi.getJoinTimeout());

                    spi.getSpiContext().addTimeoutObject(joinTimeoutObj);
                }

                rtState.joinTo = joinTimeoutObj;
            }

            if (!locNode.isClient())
                zkClient.getChildrenAsync(zkPaths.aliveNodesDir, null, new CheckCoordinatorCallback(rtState));

            zkClient.getDataAsync(zkPaths.evtsPath, rtState.watcher, rtState.watcher);

            spi.getSpiContext().addTimeoutObject(rtState.joinErrTo);
        }
        catch (IgniteCheckedException | ZookeeperClientFailedException e) {
            throw new IgniteSpiException("Failed to initialize Zookeeper nodes", e);
        }
    }

    /**
     * Authenticate local node.
     *
     * @param nodeAuth Authenticator.
     * @param locCred Local security credentials for authentication.
     * @throws IgniteSpiException If any error occurs.
     */
    private void localAuthentication(DiscoverySpiNodeAuthenticator nodeAuth, SecurityCredentials locCred){
        assert nodeAuth != null;
        assert locCred != null;

        try {
            SecurityContext subj = nodeAuth.authenticateNode(locNode, locCred);

            // Note: exception message is checked in tests.
            if (subj == null)
                throw new IgniteSpiException("Authentication failed for local node.");

            if (!(subj instanceof Serializable))
                throw new IgniteSpiException("Authentication subject is not Serializable.");

            Map<String, Object> attrs = new HashMap<>(locNode.attributes());

            attrs.put(ATTR_SECURITY_SUBJECT_V2, U.marshal(marsh, subj));

            locNode.setAttributes(attrs);
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to authenticate local node (will shutdown local node).", e);
        }
    }

    /**
     * @param node Node.
     * @param zipBytes Zip-compressed marshalled security subject.
     * @throws Exception If failed.
     */
    private void setNodeSecuritySubject(ZookeeperClusterNode node, byte[] zipBytes) throws Exception {
        assert zipBytes != null;

        Map<String, Object> attrs = new HashMap<>(node.getAttributes());

        attrs.put(ATTR_SECURITY_SUBJECT_V2, unzip(zipBytes));

        node.setAttributes(attrs);
    }

    /**
     * @param node Node.
     * @return Credentials.
     * @throws IgniteCheckedException If failed to unmarshal.
     */
    private SecurityCredentials unmarshalCredentials(ZookeeperClusterNode node) throws Exception {
        byte[] credBytes = (byte[])node.getAttributes().get(ATTR_SECURITY_CREDENTIALS);

        if (credBytes == null)
            return null;

        return unmarshalZip(credBytes);
    }

    /**
     * Marshalls credentials with discovery SPI marshaller (will replace attribute value).
     *
     * @param node Node to marshall credentials for.
     * @throws IgniteSpiException If marshalling failed.
     */
    private void marshalCredentialsOnJoin(ZookeeperClusterNode node) throws IgniteSpiException {
        try {
            // Use security-unsafe getter.
            Map<String, Object> attrs0 = node.getAttributes();

            Object creds = attrs0.get(ATTR_SECURITY_CREDENTIALS);

            if (creds != null) {
                Map<String, Object> attrs = new HashMap<>(attrs0);

                assert !(creds instanceof byte[]);

                attrs.put(ATTR_SECURITY_CREDENTIALS, marshalZip(creds));

                node.setAttributes(attrs);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal node security credentials: " + node.id(), e);
        }
    }

    /**
     *
     */
    private class UpdateProcessedEventsTimeoutObject extends ZkTimeoutObject {
        /** */
        private final ZkRuntimeState rtState;

        /**
         * @param rtState Runtime state.
         * @param timeout Timeout.
         */
        UpdateProcessedEventsTimeoutObject(ZkRuntimeState rtState, long timeout) {
            super(timeout);

            this.rtState = rtState;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            runInWorkerThread(new ZkRunnable(rtState, ZookeeperDiscoveryImpl.this) {
                @Override protected void run0() throws Exception {
                    updateProcessedEventsOnTimeout(rtState, UpdateProcessedEventsTimeoutObject.this);
                }
            });
        }
    }

    /**
     *
     */
    private class JoinTimeoutObject extends ZkTimeoutObject {
        /**
         * @param timeout Timeout.
         */
        JoinTimeoutObject(long timeout) {
            super(timeout);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (cancelled || rtState.joined)
                return;

            runInWorkerThread(new Runnable() {
                @Override public void run() {
                    synchronized (stateMux) {
                        if (cancelled || rtState.joined)
                            return;

                        if (connState == ConnectionState.STOPPED)
                            return;

                        connState = ConnectionState.STOPPED;
                    }

                    U.warn(log, "Failed to connect to cluster, either connection to ZooKeeper can not be established or there " +
                        "are no alive server nodes (consider increasing 'joinTimeout' configuration  property) [" +
                        "joinTimeout=" + spi.getJoinTimeout() + ']');

                    // Note: exception message is checked in tests.
                    onSegmented(new IgniteSpiException("Failed to connect to cluster within configured timeout"));
                }
            });
        }
    }

    /**
     *
     */
    private class CheckJoinErrorWatcher extends ZkAbstractWatcher implements AsyncCallback.DataCallback {
        /** */
        private final String joinDataPath;

        /** */
        private ZkTimeoutObject timeoutObj;

        /**
         * @param timeout Timeout.
         * @param joinDataPath0 Node joined data path.
         * @param rtState0 State.
         */
        CheckJoinErrorWatcher(long timeout, String joinDataPath0, ZkRuntimeState rtState0) {
            super(rtState0, ZookeeperDiscoveryImpl.this);

            this.joinDataPath = joinDataPath0;

            timeoutObj = new ZkTimeoutObject(timeout) {
                @Override public void onTimeout() {
                    if (rtState.errForClose != null || rtState.joined)
                        return;

                    synchronized (stateMux) {
                        if (connState != ConnectionState.STARTED)
                            return;
                    }

                    rtState.zkClient.getDataAsync(joinDataPath,
                        CheckJoinErrorWatcher.this,
                        CheckJoinErrorWatcher.this);
                }
            };
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (rc != 0)
                return;

            if (!onProcessStart())
                return;

            try {
                Object obj = unmarshalZip(data);

                if (obj instanceof ZkInternalJoinErrorMessage) {
                    ZkInternalJoinErrorMessage joinErr = (ZkInternalJoinErrorMessage)obj;

                    onSegmented(new IgniteSpiException(joinErr.err));
                }

                onProcessEnd();
            }
            catch (Throwable e) {
                onProcessError(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void process0(WatchedEvent evt) {
            if (rtState.errForClose != null || rtState.joined)
                return;

            if (evt.getType() == Event.EventType.NodeDataChanged)
                rtState.zkClient.getDataAsync(evt.getPath(), this, this);
        }
    }

    /**
     * @param aliveNodes Alive nodes.
     * @throws Exception If failed.
     */
    private void checkIsCoordinator(final List<String> aliveNodes) throws Exception {
        assert !locNode.isClient();

        TreeMap<Long, String> aliveSrvs = new TreeMap<>();

        long locInternalOrder = rtState.internalOrder;

        for (String aliveNodePath : aliveNodes) {
            if (ZkIgnitePaths.aliveNodeClientFlag(aliveNodePath))
                continue;

            Long internalId = ZkIgnitePaths.aliveInternalId(aliveNodePath);

            aliveSrvs.put(internalId, aliveNodePath);
        }

        assert !aliveSrvs.isEmpty();

        Map.Entry<Long, String> crdE = aliveSrvs.firstEntry();

        if (locInternalOrder == crdE.getKey())
            onBecomeCoordinator(aliveNodes);
        else {
            assert aliveSrvs.size() > 1 : aliveSrvs;

            Map.Entry<Long, String> prevE = aliveSrvs.floorEntry(locInternalOrder - 1);

            assert prevE != null;

            if (log.isInfoEnabled()) {
                log.info("Discovery coordinator already exists, watch for previous server node [" +
                    "locId=" + locNode.id() +
                    ", watchPath=" + prevE.getValue() + ']');
             }

            PreviousNodeWatcher watcher = new ServerPreviousNodeWatcher(rtState);

            rtState.zkClient.existsAsync(zkPaths.aliveNodesDir + "/" + prevE.getValue(), watcher, watcher);
        }
    }

    /**
     * @param aliveNodes Alive nodes.
     * @throws Exception If failed.
     */
    private void checkClientsStatus(final List<String> aliveNodes) throws Exception {
        assert locNode.isClient() : locNode;
        assert rtState.joined;
        assert rtState.evtsData != null;

        TreeMap<Long, String> aliveClients = new TreeMap<>();

        String srvPath = null;
        Long srvInternalOrder = null;

        long locInternalOrder = rtState.internalOrder;

        for (String aliveNodePath : aliveNodes) {
            Long internalId = ZkIgnitePaths.aliveInternalId(aliveNodePath);

            if (ZkIgnitePaths.aliveNodeClientFlag(aliveNodePath))
                aliveClients.put(internalId, aliveNodePath);
            else {
                if (srvInternalOrder == null || internalId < srvInternalOrder) {
                    srvPath = aliveNodePath;
                    srvInternalOrder = internalId;
                }
            }
        }

        assert !aliveClients.isEmpty();

        Map.Entry<Long, String> oldest = aliveClients.firstEntry();

        boolean oldestClient = locInternalOrder == oldest.getKey();

        if (srvPath == null) {
            if (oldestClient) {
                Stat stat = new Stat();

                ZkDiscoveryEventsData prevEvts = rtState.evtsData;

                byte[] evtsBytes = rtState.zkClient.getData(zkPaths.evtsPath, stat);

                assert evtsBytes.length > 0;

                ZkDiscoveryEventsData newEvts = unmarshalZip(evtsBytes);

                if (prevEvts.clusterId.equals(newEvts.clusterId)) {
                    U.warn(log, "All server nodes failed, notify all clients [locId=" + locNode.id() + ']');

                    try {
                        generateNoServersEvent(newEvts, stat);
                    }
                    catch (KeeperException.BadVersionException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to save no servers message. Path version changed.");

                        rtState.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, null,
                            new CheckClientsStatusCallback(rtState));
                    }
                }
                else
                    U.warn(log, "All server nodes failed (received events from new cluster).");
            }
        }
        else {
            String watchPath;

            if (oldestClient) {
                watchPath = srvPath;

                if (log.isInfoEnabled()) {
                    log.info("Servers exists, watch for server node [locId=" + locNode.id() +
                        ", watchPath=" + watchPath + ']');
                }
            }
            else {
                assert aliveClients.size() > 1 : aliveClients;

                Map.Entry<Long, String> prevE = aliveClients.floorEntry(locInternalOrder - 1);

                assert prevE != null;

                watchPath = prevE.getValue();

                if (log.isInfoEnabled()) {
                    log.info("Servers exists, watch for previous node [locId=" + locNode.id() +
                        ", watchPath=" + watchPath + ']');
                }
            }

            PreviousNodeWatcher watcher = new ClientPreviousNodeWatcher(rtState);

            rtState.zkClient.existsAsync(zkPaths.aliveNodesDir + "/" + watchPath, watcher, watcher);
        }
    }

    /**
     * @param evtsData Events data.
     * @param evtsStat Events zookeeper state.
     * @throws Exception If failed.
     */
    private void generateNoServersEvent(ZkDiscoveryEventsData evtsData, Stat evtsStat) throws Exception {
        evtsData.evtIdGen++;

        ZkDiscoveryCustomEventData evtData = new ZkDiscoveryCustomEventData(
            evtsData.evtIdGen,
            0L,
            evtsData.topVer,
            locNode.id(),
            new ZkNoServersMessage(),
            null);

        Collection<ZookeeperClusterNode> nodesToAck = Collections.emptyList();

        evtsData.addEvent(nodesToAck, evtData);

        byte[] newEvtsBytes = marshalZip(evtsData);

        rtState.zkClient.setData(zkPaths.evtsPath, newEvtsBytes, evtsStat.getVersion());
    }

    /**
     * @param lastEvts Last events from previous coordinator.
     * @throws Exception If failed.
     */
    private void previousCoordinatorCleanup(ZkDiscoveryEventsData lastEvts) throws Exception {
        for (ZkDiscoveryEventData evtData : lastEvts.evts.values()) {
            if (evtData instanceof ZkDiscoveryCustomEventData) {
                ZkDiscoveryCustomEventData evtData0 = (ZkDiscoveryCustomEventData)evtData;

                // It is possible previous coordinator failed before finished cleanup.
                if (evtData0.msg instanceof ZkCommunicationErrorResolveFinishMessage) {
                    try {
                        ZkCommunicationErrorResolveFinishMessage msg =
                            (ZkCommunicationErrorResolveFinishMessage)evtData0.msg;

                        ZkCommunicationErrorResolveResult res = unmarshalZip(
                            ZkDistributedCollectDataFuture.readResult(rtState.zkClient, zkPaths, msg.futId));

                        deleteAliveNodes(res.killedNodes);
                    }
                    catch (KeeperException.NoNodeException ignore) {
                        // No-op.
                    }
                }
                else if (evtData0.resolvedMsg instanceof ZkForceNodeFailMessage)
                    deleteAliveNode(((ZkForceNodeFailMessage)evtData0.resolvedMsg).nodeInternalId);
            }
        }
    }

    /**
     * @param aliveNodes Alive nodes paths.
     * @throws Exception If failed.
     */
    private void onBecomeCoordinator(List<String> aliveNodes) throws Exception {
        ZkDiscoveryEventsData prevEvts = processNewEvents(rtState.zkClient.getData(zkPaths.evtsPath));

        rtState.crd = true;

        if (rtState.joined) {
            if (log.isInfoEnabled())
                log.info("Node is new discovery coordinator [locId=" + locNode.id() + ']');

            assert locNode.order() > 0 : locNode;
            assert rtState.evtsData != null;

            previousCoordinatorCleanup(rtState.evtsData);

            UUID futId = rtState.evtsData.communicationErrorResolveFutureId();

            if (futId != null) {
                if (log.isInfoEnabled()) {
                    log.info("New discovery coordinator will handle already started cluster-wide communication " +
                        "error resolve [reqId=" + futId + ']');
                }

                ZkCommunicationErrorProcessFuture fut = commErrProcFut.get();

                ZkDistributedCollectDataFuture collectResFut = collectCommunicationStatusFuture(futId);

                if (fut != null)
                    fut.nodeResultCollectFuture(collectResFut);
            }

            for (ZkDiscoveryEventData evtData : rtState.evtsData.evts.values())
                evtData.initRemainingAcks(rtState.top.nodesByOrder.values());

            handleProcessedEvents("crd");
        }
        else {
            String locAlivePath = rtState.locNodeZkPath.substring(rtState.locNodeZkPath.lastIndexOf('/') + 1);

            deleteJoiningNodeData(locNode.id(),
                ZkIgnitePaths.aliveNodePrefixId(locAlivePath),
                rtState.joinDataPartCnt);

            DiscoverySpiNodeAuthenticator nodeAuth = spi.getAuthenticator();

            if (nodeAuth != null) {
                try {
                    if (log.isInfoEnabled()) {
                        log.info("Node is first server node in cluster, try authenticate local node " +
                            "[locId=" + locNode.id() + ']');
                    }

                    localAuthentication(nodeAuth, unmarshalCredentials(locNode));
                }
                catch (Exception e) {
                    U.warn(log, "Local node authentication failed: " + e, e);

                    onSegmented(e);

                    // Stop any further processing.
                    throw new ZookeeperClientFailedException("Local node authentication failed: " + e);
                }
            }

            newClusterStarted(prevEvts);
        }

        rtState.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, rtState.watcher, rtState.watcher);
        rtState.zkClient.getChildrenAsync(zkPaths.customEvtsDir, rtState.watcher, rtState.watcher);

        for (String alivePath : aliveNodes)
            watchAliveNodeData(alivePath);
    }

    /**
     * @param alivePath Node path.
     */
    private void watchAliveNodeData(String alivePath) {
        assert rtState.locNodeZkPath != null;

        String path = zkPaths.aliveNodesDir + "/" + alivePath;

        if (!path.equals(rtState.locNodeZkPath))
            rtState.zkClient.getDataAsync(path, rtState.aliveNodeDataWatcher, rtState.aliveNodeDataWatcher);
    }

    /**
     * @param aliveNodes ZK nodes representing alive cluster nodes.
     * @throws Exception If failed.
     */
    private void generateTopologyEvents(List<String> aliveNodes) throws Exception {
        assert rtState.crd;

        if (log.isInfoEnabled())
            log.info("Process alive nodes change [alives=" + aliveNodes.size() + "]");

        if (rtState.updateAlives) {
            aliveNodes = rtState.zkClient.getChildren(zkPaths.aliveNodesDir);

            rtState.updateAlives = false;
        }

        TreeMap<Long, String> alives = new TreeMap<>();

        for (String child : aliveNodes) {
            Long internalId = ZkIgnitePaths.aliveInternalId(child);

            Object old = alives.put(internalId, child);

            assert old == null;
        }

        TreeMap<Long, ZookeeperClusterNode> curTop = new TreeMap<>(rtState.top.nodesByOrder);

        int newEvts = 0;

        final int MAX_NEW_EVTS = IgniteSystemProperties.getInteger(IGNITE_ZOOKEEPER_DISCOVERY_SPI_MAX_EVTS, 100);

        List<ZookeeperClusterNode> failedNodes = null;

        for (Map.Entry<Long, ZookeeperClusterNode> e : rtState.top.nodesByInternalId.entrySet()) {
            if (!alives.containsKey(e.getKey())) {
                ZookeeperClusterNode failedNode = e.getValue();

                if (failedNodes == null)
                    failedNodes = new ArrayList<>();

                failedNodes.add(failedNode);

                generateNodeFail(curTop, failedNode);

                newEvts++;

                if (newEvts == MAX_NEW_EVTS) {
                    saveAndProcessNewEvents();

                    if (log.isInfoEnabled()) {
                        log.info("Delay alive nodes change process, max event threshold reached [newEvts=" + newEvts +
                            ", totalEvts=" + rtState.evtsData.evts.size() + ']');
                    }

                    handleProcessedEventsOnNodesFail(failedNodes);

                    throttleNewEventsGeneration();

                    rtState.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, rtState.watcher, rtState.watcher);

                    return;
                }
            }
        }

        // Process failures before processing join, otherwise conflicts are possible in case of fast node stop/re-start.
        if (newEvts > 0) {
            saveAndProcessNewEvents();

            handleProcessedEventsOnNodesFail(failedNodes);

            rtState.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, rtState.watcher, rtState.watcher);

            return;
        }

        generateJoinEvents(curTop, alives, MAX_NEW_EVTS);

        if (failedNodes != null)
            handleProcessedEventsOnNodesFail(failedNodes);
    }

    /**
     * @param curTop Current topology.
     * @param alives Alive znodes.
     * @param MAX_NEW_EVTS Max event to process.
     * @throws Exception If failed.
     */
    private void generateJoinEvents(TreeMap<Long, ZookeeperClusterNode> curTop,
        TreeMap<Long, String> alives,
        final int MAX_NEW_EVTS) throws Exception
    {
       ZkBulkJoinContext joinCtx = new ZkBulkJoinContext();

       for (Map.Entry<Long, String> e : alives.entrySet()) {
           Long internalId = e.getKey();

           if (!rtState.top.nodesByInternalId.containsKey(internalId)) {
               UUID rslvFutId = rtState.evtsData.communicationErrorResolveFutureId();

               if (rslvFutId != null) {
                   if (log.isInfoEnabled()) {
                       log.info("Delay alive nodes change process while communication error resolve " +
                           "is in progress [reqId=" + rslvFutId + ']');
                   }

                   break;
               }

               processJoinOnCoordinator(joinCtx, curTop, internalId, e.getValue());

               if (joinCtx.nodes() == MAX_NEW_EVTS) {
                   generateBulkJoinEvent(curTop, joinCtx);

                   if (log.isInfoEnabled()) {
                       log.info("Delay alive nodes change process, max event threshold reached [" +
                           "newEvts=" + joinCtx.nodes() +
                           ", totalEvts=" + rtState.evtsData.evts.size() + ']');
                   }

                   throttleNewEventsGeneration();

                   rtState.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, rtState.watcher, rtState.watcher);

                   return;
               }
           }
       }

       if (joinCtx.nodes() > 0)
           generateBulkJoinEvent(curTop, joinCtx);
    }

    /**
     * @param curTop Current topology.
     * @param joinCtx Joined nodes context.
     * @throws Exception If failed.
     */
    private void generateBulkJoinEvent(TreeMap<Long, ZookeeperClusterNode> curTop, ZkBulkJoinContext joinCtx)
        throws Exception
    {
        rtState.evtsData.evtIdGen++;

        long evtId = rtState.evtsData.evtIdGen;

        List<T2<ZkJoinedNodeEvtData, Map<Integer, Serializable>>> nodes = joinCtx.nodes;

        assert nodes != null && nodes.size() > 0;

        int nodeCnt = nodes.size();

        List<ZkJoinedNodeEvtData> joinedNodes = new ArrayList<>(nodeCnt);

        Map<Long, byte[]> discoDataMap = U.newHashMap(nodeCnt);
        Map<Long, Long> dupDiscoData = null;

        for (int i = 0; i < nodeCnt; i++) {
            T2<ZkJoinedNodeEvtData, Map<Integer, Serializable>> nodeEvtData = nodes.get(i);

            Map<Integer, Serializable> discoData = nodeEvtData.get2();

            byte[] discoDataBytes = U.marshal(marsh, discoData);

            Long dupDataNode = null;

            for (Map.Entry<Long, byte[]> e : discoDataMap.entrySet()) {
                if (Arrays.equals(discoDataBytes, e.getValue())) {
                    dupDataNode = e.getKey();

                    break;
                }
            }

            long nodeTopVer = nodeEvtData.get1().topVer;

            if (dupDataNode != null) {
                if (dupDiscoData == null)
                    dupDiscoData = new HashMap<>();

                Long old = dupDiscoData.put(nodeTopVer, dupDataNode);

                assert old == null : old;
            }
            else
                discoDataMap.put(nodeTopVer, discoDataBytes);

            joinedNodes.add(nodeEvtData.get1());
        }

        int overhead = 5;

        ZkJoinEventDataForJoined dataForJoined = new ZkJoinEventDataForJoined(
            new ArrayList<>(curTop.values()),
            discoDataMap,
            dupDiscoData);

        byte[] dataForJoinedBytes = marshalZip(dataForJoined);

        long addDataStart = System.currentTimeMillis();

        int dataForJoinedPartCnt = saveData(zkPaths.joinEventDataPathForJoined(evtId),
            dataForJoinedBytes,
            overhead);

        long addDataTime = System.currentTimeMillis() - addDataStart;

        ZkDiscoveryNodeJoinEventData evtData = new ZkDiscoveryNodeJoinEventData(
            evtId,
            rtState.evtsData.topVer,
            joinedNodes,
            dataForJoinedPartCnt);

        rtState.evtsData.addEvent(curTop.values(), evtData);

        if (log.isInfoEnabled()) {
            if (nodeCnt > 1) {
                log.info("Generated NODE_JOINED bulk event [" +
                    "nodeCnt=" + nodeCnt +
                    ", dataForJoinedSize=" + dataForJoinedBytes.length +
                    ", dataForJoinedPartCnt=" + dataForJoinedPartCnt +
                    ", addDataTime=" + addDataTime +
                    ", evt=" + evtData + ']');
            }
            else {
                log.info("Generated NODE_JOINED event [" +
                    "dataForJoinedSize=" + dataForJoinedBytes.length +
                    ", dataForJoinedPartCnt=" + dataForJoinedPartCnt +
                    ", addDataTime=" + addDataTime +
                    ", evt=" + evtData + ']');
            }
        }

        saveAndProcessNewEvents();
    }

    /**
     *
     */
    private void throttleNewEventsGeneration() {
        long delay = IgniteSystemProperties.getLong(IGNITE_ZOOKEEPER_DISCOVERY_SPI_EVTS_THROTTLE, 0);

        if (delay > 0) {
            if (log.isInfoEnabled())
                log.info("Sleep delay before generate new events [delay=" + delay + ']');

            try {
                Thread.sleep(delay);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @param prefixId Path prefix.
     * @return Join data.
     * @throws Exception If failed.
     */
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
     * @param nodeId Node ID.
     * @param prefixId Path prefix.
     * @param aliveNodePath Node path.
     * @return Join data.
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
     * @param joinCtx Joined nodes context.
     * @param curTop Current nodes.
     * @param internalId Joined node internal ID.
     * @param aliveNodePath Joined node path.
     * @throws Exception If failed.
     */
    private void processJoinOnCoordinator(
        ZkBulkJoinContext joinCtx,
        TreeMap<Long, ZookeeperClusterNode> curTop,
        long internalId,
        String aliveNodePath)
        throws Exception
    {
        UUID nodeId = ZkIgnitePaths.aliveNodeId(aliveNodePath);
        UUID prefixId = ZkIgnitePaths.aliveNodePrefixId(aliveNodePath);

        Object data = unmarshalJoinDataOnCoordinator(nodeId, prefixId, aliveNodePath);

        if (data instanceof ZkJoiningNodeData) {
            ZkJoiningNodeData joiningNodeData = (ZkJoiningNodeData)data;

            ZkNodeValidateResult validateRes = validateJoiningNode(joiningNodeData.node());

            if (validateRes.err == null) {
                ZookeeperClusterNode joinedNode = joiningNodeData.node();

                assert nodeId.equals(joinedNode.id()) : joiningNodeData.node();

                addJoinedNode(
                    joinCtx,
                    curTop,
                    joiningNodeData,
                    internalId,
                    prefixId,
                    validateRes.secSubjZipBytes);

                watchAliveNodeData(aliveNodePath);
            }
            else {
                ZkInternalJoinErrorMessage joinErr = new ZkInternalJoinErrorMessage(
                    ZkIgnitePaths.aliveInternalId(aliveNodePath),
                    validateRes.err);

                processJoinError(aliveNodePath, nodeId, prefixId, joinErr);
            }
        }
        else {
            assert data instanceof ZkInternalJoinErrorMessage : data;

            processJoinError(aliveNodePath, nodeId, prefixId, (ZkInternalJoinErrorMessage)data);
        }
    }

    /**
     * @param aliveNodePath Joined node path.
     * @param nodeId Node ID.
     * @param prefixId Path prefix ID.
     * @param joinErr Join error message.
     * @throws Exception If failed.
     */
    private void processJoinError(String aliveNodePath,
        UUID nodeId,
        UUID prefixId,
        ZkInternalJoinErrorMessage joinErr) throws Exception {
        ZookeeperClient client = rtState.zkClient;

        if (joinErr.notifyNode) {
            String joinDataPath = zkPaths.joiningNodeDataPath(nodeId, prefixId);

            client.setData(joinDataPath, marshalZip(joinErr), -1);

            client.deleteIfExists(zkPaths.aliveNodesDir + "/" + aliveNodePath, -1);
        }
        else {
            if (log.isInfoEnabled())
                log.info("Ignore join data, node was failed by previous coordinator: " + aliveNodePath);

            client.deleteIfExists(zkPaths.aliveNodesDir + "/" + aliveNodePath, -1);
        }
    }

    /**
     * @param node Joining node.
     * @return Validation result.
     */
    private ZkNodeValidateResult validateJoiningNode(ZookeeperClusterNode node) {
        ZookeeperClusterNode node0 = rtState.top.nodesById.get(node.id());

        if (node0 != null) {
            U.error(log, "Failed to include node in cluster, node with the same ID already exists [joiningNode=" + node +
                ", existingNode=" + node0 + ']');

            // Note: exception message is checked in tests.
            return new ZkNodeValidateResult("Node with the same ID already exists: " + node0);
        }

        ZkNodeValidateResult res = authenticateNode(node);

        if (res.err != null)
            return res;

        IgniteNodeValidationResult err = spi.getSpiContext().validateNode(node);

        if (err != null) {
            LT.warn(log, err.message());

            res.err = err.sendMessage();
        }

        return res;
    }

    /**
     * @param node Node.
     * @return Validation result.
     */
    private ZkNodeValidateResult authenticateNode(ZookeeperClusterNode node) {
        DiscoverySpiNodeAuthenticator nodeAuth = spi.getAuthenticator();

        if (nodeAuth == null)
            return new ZkNodeValidateResult((byte[])null);

        SecurityCredentials cred;

        try {
            cred = unmarshalCredentials(node);
        }
        catch (Exception e) {
            U.error(log, "Failed to unmarshal node credentials: " + e, e);

            return new ZkNodeValidateResult("Failed to unmarshal node credentials");
        }

        SecurityContext subj = nodeAuth.authenticateNode(node, cred);

        if (subj == null) {
            U.warn(log, "Authentication failed [nodeId=" + node.id() +
                    ", addrs=" + U.addressesAsString(node) + ']',
                "Authentication failed [nodeId=" + U.id8(node.id()) + ", addrs=" +
                    U.addressesAsString(node) + ']');

            // Note: exception message test is checked in tests.
            return new ZkNodeValidateResult("Authentication failed");
        }

        if (!(subj instanceof Serializable)) {
            U.warn(log, "Authentication subject is not Serializable [nodeId=" + node.id() +
                    ", addrs=" + U.addressesAsString(node) + ']',
                "Authentication subject is not Serializable [nodeId=" + U.id8(node.id()) +
                    ", addrs=" +
                    U.addressesAsString(node) + ']');

            return new ZkNodeValidateResult("Authentication subject is not serializable");
        }

        byte[] secSubjZipBytes;

        try {
            secSubjZipBytes = marshalZip(subj);
        }
        catch (Exception e) {
            U.error(log, "Failed to marshal node security subject: " + e, e);

            return new ZkNodeValidateResult("Failed to marshal node security subject");
        }

        return new ZkNodeValidateResult(secSubjZipBytes);
    }

    /**
     * @throws Exception If failed.
     */
    private void saveAndProcessNewEvents() throws Exception {
        if (stopping())
            return;

        long start = System.currentTimeMillis();

        byte[] evtsBytes = marshalZip(rtState.evtsData);

        rtState.zkClient.setData(zkPaths.evtsPath, evtsBytes, -1);

        long time = System.currentTimeMillis() - start;

        if (prevSavedEvtsTopVer != rtState.evtsData.topVer) {
            if (log.isInfoEnabled()) {
                log.info("Discovery coordinator saved new topology events [topVer=" + rtState.evtsData.topVer +
                    ", size=" + evtsBytes.length +
                    ", evts=" + rtState.evtsData.evts.size() +
                    ", lastEvt=" + rtState.evtsData.evtIdGen +
                    ", saveTime=" + time + ']');
            }

            prevSavedEvtsTopVer = rtState.evtsData.topVer;
        }
        else if (log.isDebugEnabled()) {
            log.debug("Discovery coordinator saved new topology events [topVer=" + rtState.evtsData.topVer +
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
     * @param joiningNodeData Join data.
     * @param internalId Joined node internal ID.
     * @param prefixId Unique path prefix.
     * @param secSubjZipBytes Marshalled security subject.
     * @throws Exception If failed.
     */
    private void addJoinedNode(
        ZkBulkJoinContext joinCtx,
        TreeMap<Long, ZookeeperClusterNode> curTop,
        ZkJoiningNodeData joiningNodeData,
        long internalId,
        UUID prefixId,
        @Nullable byte[] secSubjZipBytes)
        throws Exception
    {
        ZookeeperClusterNode joinedNode = joiningNodeData.node();

        UUID nodeId = joinedNode.id();

        rtState.evtsData.topVer++;

        joinedNode.order(rtState.evtsData.topVer);
        joinedNode.internalId(internalId);

        DiscoveryDataBag joiningNodeBag = new DiscoveryDataBag(nodeId, joiningNodeData.node().isClient());

        joiningNodeBag.joiningNodeData(joiningNodeData.discoveryData());

        exchange.onExchange(joiningNodeBag);

        DiscoveryDataBag collectBag = new DiscoveryDataBag(nodeId,
            new HashSet<Integer>(),
            joiningNodeData.node().isClient());

        collectBag.joiningNodeData(joiningNodeBag.joiningNodeData());

        exchange.collect(collectBag);

        Map<Integer, Serializable> commonData = collectBag.commonData();

        Object old = curTop.put(joinedNode.order(), joinedNode);

        assert old == null;

        int overhead = 5;

        int secSubjPartCnt = 0;

        if (secSubjZipBytes != null) {
            secSubjPartCnt = saveData(zkPaths.joinEventSecuritySubjectPath(joinedNode.order()),
                secSubjZipBytes,
                overhead);

            assert secSubjPartCnt > 0 : secSubjPartCnt;

            setNodeSecuritySubject(joinedNode, secSubjZipBytes);
        }

        ZkJoinedNodeEvtData nodeEvtData = new ZkJoinedNodeEvtData(
            rtState.evtsData.topVer,
            joinedNode.id(),
            joinedNode.internalId(),
            prefixId,
            joiningNodeData.partCount(),
            secSubjPartCnt);

        nodeEvtData.joiningNodeData = joiningNodeData;

        joinCtx.addJoinedNode(nodeEvtData, commonData);

        rtState.evtsData.onNodeJoin(joinedNode);

        stats.onNodeJoined();
    }

    /**
     * @param path Path to save.
     * @param bytes Bytes to save.
     * @param overhead Extra overhead.
     * @return Parts count.
     * @throws Exception If failed.
     */
    private int saveData(String path, byte[] bytes, int overhead) throws Exception {
        int dataForJoinedPartCnt = 1;

        if (rtState.zkClient.needSplitNodeData(path, bytes, overhead)) {
            dataForJoinedPartCnt = saveMultipleParts(rtState.zkClient,
                path,
                rtState.zkClient.splitNodeData(path, bytes, overhead));
        }
        else {
            rtState.zkClient.createIfNeeded(multipartPathName(path, 0),
                bytes,
                PERSISTENT);
        }

        return dataForJoinedPartCnt;
    }

    /**
     * @param prevEvts Events from previous cluster.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void newClusterStarted(@Nullable ZkDiscoveryEventsData prevEvts) throws Exception {
        assert !locNode.isClient() : locNode;

        long locInternalId = rtState.internalOrder;

        assert prevEvts == null || prevEvts.maxInternalOrder < locInternalId;

        spi.getSpiContext().removeTimeoutObject(rtState.joinErrTo);

        cleanupPreviousClusterData(prevEvts != null ? prevEvts.maxInternalOrder + 1 : -1L);

        rtState.joined = true;
        rtState.gridStartTime = System.currentTimeMillis();

        rtState.evtsData = ZkDiscoveryEventsData.createForNewCluster(rtState.gridStartTime);

        if (log.isInfoEnabled()) {
            log.info("New cluster started [locId=" + locNode.id() +
                ", clusterId=" + rtState.evtsData.clusterId +
                ", startTime=" + rtState.evtsData.clusterStartTime + ']');
        }

        locNode.internalId(locInternalId);
        locNode.order(1);

        rtState.evtsData.onNodeJoin(locNode);

        rtState.top.addNode(locNode);

        final List<ClusterNode> topSnapshot = Collections.singletonList((ClusterNode)locNode);

        lsnr.onDiscovery(EVT_NODE_JOINED,
            1L,
            locNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            null);

        // Reset events (this is also notification for clients left from previous cluster).
        rtState.zkClient.setData(zkPaths.evtsPath, marshalZip(rtState.evtsData), -1);

        joinFut.onDone();
    }

    /**
     * @param startInternalOrder Starting internal order for cluster (znodes having lower order belong
     *      to clients from previous cluster and should be removed).

     * @throws Exception If failed.
     */
    private void cleanupPreviousClusterData(long startInternalOrder) throws Exception {
        long start = System.currentTimeMillis();

        ZookeeperClient client = rtState.zkClient;

        // TODO ZK: use multi, better batching + max-size safe + NoNodeException safe.
        List<String> evtChildren = rtState.zkClient.getChildren(zkPaths.evtsPath);

        for (String evtPath : evtChildren) {
            String evtDir = zkPaths.evtsPath + "/" + evtPath;

            removeChildren(evtDir);
        }

        client.deleteAll(zkPaths.evtsPath, evtChildren, -1);

        client.deleteAll(zkPaths.customEvtsDir,
            client.getChildren(zkPaths.customEvtsDir),
            -1);

        rtState.zkClient.deleteAll(zkPaths.customEvtsPartsDir,
            rtState.zkClient.getChildren(zkPaths.customEvtsPartsDir),
            -1);

        rtState.zkClient.deleteAll(zkPaths.customEvtsAcksDir,
            rtState.zkClient.getChildren(zkPaths.customEvtsAcksDir),
            -1);

        if (startInternalOrder > 0) {
            for (String alive : rtState.zkClient.getChildren(zkPaths.aliveNodesDir)) {
                if (ZkIgnitePaths.aliveInternalId(alive) < startInternalOrder)
                    rtState.zkClient.deleteIfExists(zkPaths.aliveNodesDir + "/" + alive, -1);
            }
        }

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

    /**
     * @param zkClient Client.
     * @param evtPath Event path.
     * @param sndNodeId Sender node ID.
     * @return Event data.
     * @throws Exception If failed.
     */
    private byte[] readCustomEventData(ZookeeperClient zkClient, String evtPath, UUID sndNodeId) throws Exception {
        int partCnt = ZkIgnitePaths.customEventPartsCount(evtPath);

        if (partCnt > 1) {
            String partsBasePath = zkPaths.customEventPartsBasePath(
                ZkIgnitePaths.customEventPrefix(evtPath), sndNodeId);

            return readMultipleParts(zkClient, partsBasePath, partCnt);
        }
        else
            return zkClient.getData(zkPaths.customEvtsDir + "/" + evtPath);
    }

    /**
     * @param customEvtNodes ZK nodes representing custom events to process.
     * @throws Exception If failed.
     */
    private void generateCustomEvents(List<String> customEvtNodes) throws Exception {
        assert rtState.crd;

        ZookeeperClient zkClient = rtState.zkClient;
        ZkDiscoveryEventsData evtsData = rtState.evtsData;

        TreeMap<Integer, String> unprocessedEvts = null;

        for (int i = 0; i < customEvtNodes.size(); i++) {
            String evtPath = customEvtNodes.get(i);

            int evtSeq = ZkIgnitePaths.customEventSequence(evtPath);

            if (evtSeq > evtsData.procCustEvt) {
                if (unprocessedEvts == null)
                    unprocessedEvts = new TreeMap<>();

                unprocessedEvts.put(evtSeq, evtPath);
            }
        }

        if (unprocessedEvts == null)
            return;

        for (Map.Entry<Integer, String> evtE : unprocessedEvts.entrySet()) {
            evtsData.procCustEvt = evtE.getKey();

            String evtPath = evtE.getValue();

            UUID sndNodeId = ZkIgnitePaths.customEventSendNodeId(evtPath);

            ZookeeperClusterNode sndNode = rtState.top.nodesById.get(sndNodeId);

            if (sndNode != null) {
                byte[] evtBytes = readCustomEventData(zkClient, evtPath, sndNodeId);

                DiscoverySpiCustomMessage msg;

                try {
                    msg = unmarshalZip(evtBytes);
                }
                catch (Exception e) {
                    U.error(log, "Failed to unmarshal custom discovery message: " + e, e);

                    deleteCustomEventDataAsync(rtState.zkClient, evtPath);

                    continue;
                }

                generateAndProcessCustomEventOnCoordinator(evtPath, sndNode, msg);
            }
            else {
                U.warn(log, "Ignore custom event from unknown node: " + sndNodeId);

                deleteCustomEventDataAsync(rtState.zkClient, evtPath);
            }
        }
    }

    /**
     * @param evtPath Event data path.
     * @param sndNode Sender node.
     * @param msg Message instance.
     * @throws Exception If failed.
     */
    private void generateAndProcessCustomEventOnCoordinator(String evtPath,
        ZookeeperClusterNode sndNode,
        DiscoverySpiCustomMessage msg) throws Exception
    {
        ZookeeperClient zkClient = rtState.zkClient;
        ZkDiscoveryEventsData evtsData = rtState.evtsData;

        ZookeeperClusterNode failedNode = null;

        if (msg instanceof ZkForceNodeFailMessage) {
            ZkForceNodeFailMessage msg0 = (ZkForceNodeFailMessage)msg;

            failedNode = rtState.top.nodesByInternalId.get(msg0.nodeInternalId);

            if (failedNode != null)
                evtsData.topVer++;
            else {
                if (log.isDebugEnabled())
                    log.debug("Ignore forcible node fail request for unknown node: " + msg0.nodeInternalId);

                deleteCustomEventDataAsync(zkClient, evtPath);

                return;
            }
        }
        else if (msg instanceof ZkCommunicationErrorResolveStartMessage) {
            ZkCommunicationErrorResolveStartMessage msg0 =
                (ZkCommunicationErrorResolveStartMessage)msg;

            if (evtsData.communicationErrorResolveFutureId() != null) {
                if (log.isInfoEnabled()) {
                    log.info("Ignore communication error resolve message, resolve process " +
                        "already started [sndNode=" + sndNode + ']');
                }

                deleteCustomEventDataAsync(zkClient, evtPath);

                return;
            }
            else {
                if (log.isInfoEnabled()) {
                    log.info("Start cluster-wide communication error resolve [sndNode=" + sndNode +
                        ", reqId=" + msg0.id +
                        ", topVer=" + evtsData.topVer + ']');
                }

                zkClient.createIfNeeded(zkPaths.distributedFutureBasePath(msg0.id),
                    null,
                    PERSISTENT);

                evtsData.communicationErrorResolveFutureId(msg0.id);
            }
        }

        evtsData.evtIdGen++;

        ZkDiscoveryCustomEventData evtData = new ZkDiscoveryCustomEventData(
            evtsData.evtIdGen,
            0L,
            evtsData.topVer,
            sndNode.id(),
            null,
            evtPath);

        evtData.resolvedMsg = msg;

        if (log.isDebugEnabled())
            log.debug("Generated CUSTOM event [evt=" + evtData + ", msg=" + msg + ']');

        boolean fastStopProcess = false;

        if (msg instanceof ZkInternalMessage)
            processInternalMessage(evtData, (ZkInternalMessage)msg);
        else {
            notifyCustomEvent(evtData, msg);

            if (msg.stopProcess()) {
                if (log.isDebugEnabled())
                    log.debug("Fast stop process custom event [evt=" + evtData + ", msg=" + msg + ']');

                fastStopProcess = true;

                // No need to process this event on others nodes, skip this event.
                evtsData.evts.remove(evtData.eventId());

                evtsData.evtIdGen--;

                DiscoverySpiCustomMessage ack = msg.ackMessage();

                if (ack != null) {
                    evtData = createAckEvent(ack, evtData);

                    if (log.isDebugEnabled())
                        log.debug("Generated CUSTOM event (ack for fast stop process) [evt=" + evtData + ", msg=" + msg + ']');

                    notifyCustomEvent(evtData, ack);
                }
                else
                    evtData = null;
            }
        }

        if (evtData != null) {
            evtsData.addEvent(rtState.top.nodesByOrder.values(), evtData);

            rtState.locNodeInfo.lastProcEvt = evtData.eventId();

            saveAndProcessNewEvents();

            if (fastStopProcess)
                deleteCustomEventDataAsync(zkClient, evtPath);

            if (failedNode != null) {
                deleteAliveNode(failedNode.internalId());

                handleProcessedEventsOnNodesFail(Collections.singletonList(failedNode));

                rtState.updateAlives = true;
            }
        }
    }

    /**
     * @param internalId Node internal ID.
     * @throws Exception If failed.
     */
    private void deleteAliveNode(long internalId) throws Exception {
        for (String child : rtState.zkClient.getChildren(zkPaths.aliveNodesDir)) {
            if (ZkIgnitePaths.aliveInternalId(child) == internalId) {
                // Need use sync delete to do not process again join of this node again.
                rtState.zkClient.deleteIfExists(zkPaths.aliveNodesDir + "/" + child, -1);

                return;
            }
        }
    }

    /**
     * @param zkClient Client.
     * @param evtPath Event path.
     */
    private void deleteCustomEventDataAsync(ZookeeperClient zkClient, String evtPath) {
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
     * @return Events.
     */
    @Nullable private ZkDiscoveryEventsData processNewEvents(byte[] data) throws Exception {
        ZkDiscoveryEventsData newEvts = data.length > 0 ? (ZkDiscoveryEventsData)unmarshalZip(data) : null;

        if (rtState.joined && (newEvts == null || !rtState.evtsData.clusterId.equals(newEvts.clusterId))) {
            assert locNode.isClient() : locNode;

            throw localNodeFail("All server nodes failed, client node disconnected (received events from new custer) " +
                "[locId=" + locNode.id() + ']', true);
        }

        if (newEvts == null)
            return null;

        assert !rtState.crd;

        // Need keep processed custom events since they contain message object which is needed to create ack.
        if (!locNode.isClient() && rtState.evtsData != null) {
            for (Map.Entry<Long, ZkDiscoveryEventData> e : rtState.evtsData.evts.entrySet()) {
                ZkDiscoveryEventData evtData = e.getValue();

                if (evtData.eventType() == ZkDiscoveryEventData.ZK_EVT_CUSTOM_EVT) {
                    ZkDiscoveryCustomEventData evtData0 =
                        (ZkDiscoveryCustomEventData)newEvts.evts.get(evtData.eventId());

                    if (evtData0 != null)
                        evtData0.resolvedMsg = ((ZkDiscoveryCustomEventData)evtData).resolvedMsg;
                }
            }
        }

        processNewEvents(newEvts);

        return newEvts;
    }

    /**
     * @param evtsData Events.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processNewEvents(final ZkDiscoveryEventsData evtsData) throws Exception {
        TreeMap<Long, ZkDiscoveryEventData> evts = evtsData.evts;

        ZookeeperClient zkClient = rtState.zkClient;

        boolean evtProcessed = false;
        boolean updateNodeInfo = false;

        try {
            for (ZkDiscoveryEventData evtData : evts.tailMap(rtState.locNodeInfo.lastProcEvt, false).values()) {
                if (log.isDebugEnabled())
                    log.debug("New discovery event data [evt=" + evtData + ", evtsHist=" + evts.size() + ']');

                switch (evtData.eventType()) {
                    case ZkDiscoveryEventData.ZK_EVT_NODE_JOIN: {
                        evtProcessed = processBulkJoin(evtsData, (ZkDiscoveryNodeJoinEventData)evtData);

                        break;
                    }

                    case ZkDiscoveryEventData.ZK_EVT_NODE_FAILED: {
                        if (!rtState.joined)
                            break;

                        evtProcessed = true;

                        notifyNodeFail((ZkDiscoveryNodeFailEventData)evtData);

                        break;
                    }

                    case ZkDiscoveryEventData.ZK_EVT_CUSTOM_EVT: {
                        if (!rtState.joined)
                            break;

                        evtProcessed = true;

                        ZkDiscoveryCustomEventData evtData0 = (ZkDiscoveryCustomEventData)evtData;

                        if (evtData0.ackEvent() && evtData0.topologyVersion() < locNode.order())
                            break;

                        DiscoverySpiCustomMessage msg;

                        if (rtState.crd) {
                            assert evtData0.resolvedMsg != null : evtData0;

                            msg = evtData0.resolvedMsg;
                        }
                        else {
                            if (evtData0.msg == null) {
                                if (evtData0.ackEvent()) {
                                    String path = zkPaths.ackEventDataPath(evtData0.origEvtId);

                                    msg = unmarshalZip(zkClient.getData(path));
                                }
                                else {
                                    assert evtData0.evtPath != null : evtData0;

                                    byte[] msgBytes = readCustomEventData(zkClient,
                                        evtData0.evtPath,
                                        evtData0.sndNodeId);

                                    msg = unmarshalZip(msgBytes);
                                }
                            }
                            else
                                msg = evtData0.msg;

                            evtData0.resolvedMsg = msg;
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

                if (rtState.joined) {
                    rtState.locNodeInfo.lastProcEvt = evtData.eventId();

                    rtState.procEvtCnt++;

                    if (rtState.procEvtCnt % evtsAckThreshold == 0)
                        updateNodeInfo = true;
                }
            }
        }
        catch (KeeperException.NoNodeException e) {
            // Can get NoNodeException if local node was forcible failed,
            // in this case coordinator does not wait when this node process all events.
            boolean exists;

            try {
                exists = rtState.zkClient.exists(rtState.locNodeZkPath);
            }
            catch (Exception e0) {
                if (log.isDebugEnabled())
                    log.debug("Failed to check is local node is alive:" + e0);

                exists = true;
            }

            if (!exists) {
                U.warn(log, "Failed to process discovery event, local node was forced to stop.", e);

                throw localNodeFail("Local node was forced to stop.", true);
            }

            throw e;
        }

        if (rtState.joined)
            rtState.evtsData = evtsData;

        if (rtState.crd)
            handleProcessedEvents("procEvt");
        else
            onEventProcessed(rtState, updateNodeInfo, evtProcessed);

        ZkCommunicationErrorProcessFuture commErrFut = commErrProcFut.get();

        if (commErrFut != null)
            commErrFut.onTopologyChange(rtState.top); // This can add new event, notify out of event process loop.
    }

    private boolean processBulkJoin(ZkDiscoveryEventsData evtsData, ZkDiscoveryNodeJoinEventData evtData)
        throws Exception
    {
        boolean evtProcessed = false;

        for (int i = 0; i < evtData.joinedNodes.size(); i++) {
            ZkJoinedNodeEvtData joinedEvtData = evtData.joinedNodes.get(i);

            if (!rtState.joined) {
                UUID joinedId = joinedEvtData.nodeId;

                boolean locJoin = joinedEvtData.joinedInternalId == rtState.internalOrder;

                if (locJoin) {
                    assert locNode.id().equals(joinedId);

                    processLocalJoin(evtsData, joinedEvtData, evtData);

                    evtProcessed = true;
                }
            }
            else {
                ZkJoiningNodeData joiningData;

                if (rtState.crd) {
                    assert joinedEvtData.joiningNodeData != null;

                    joiningData = joinedEvtData.joiningNodeData;
                }
                else {
                    joiningData = unmarshalJoinData(joinedEvtData.nodeId, joinedEvtData.joinDataPrefixId);

                    DiscoveryDataBag dataBag = new DiscoveryDataBag(joinedEvtData.nodeId, joiningData.node().isClient());

                    dataBag.joiningNodeData(joiningData.discoveryData());

                    exchange.onExchange(dataBag);
                }

                if (joinedEvtData.secSubjPartCnt > 0 && joiningData.node().attribute(ATTR_SECURITY_SUBJECT_V2) == null)
                    readAndInitSecuritySubject(joiningData.node(), joinedEvtData);

                notifyNodeJoin(joinedEvtData, joiningData);
            }
        }

        return evtProcessed;
    }

    /**
     * @param rtState Runtime state.
     * @param updateNodeInfo {@code True} if need update processed events without delay.
     * @param evtProcessed {@code True} if new event was processed.
     * @throws Exception If failed.
     */
    private void onEventProcessed(ZkRuntimeState rtState,
        boolean updateNodeInfo,
        boolean evtProcessed) throws Exception
    {
        synchronized (stateMux) {
            if (updateNodeInfo) {
                assert rtState.locNodeZkPath != null;

                if (log.isDebugEnabled())
                    log.debug("Update processed events: " + rtState.locNodeInfo.lastProcEvt);

                updateProcessedEvents(rtState);

                if (rtState.procEvtsUpdateTo != null) {
                    spi.getSpiContext().removeTimeoutObject(rtState.procEvtsUpdateTo);

                    rtState.procEvtsUpdateTo = null;
                }
            }
            else if (evtProcessed) {
                rtState.locNodeInfo.needUpdate = true;

                if (rtState.procEvtsUpdateTo == null) {
                    long updateTimeout = IgniteSystemProperties.getLong(IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_TIMEOUT,
                        60_000);

                    if (updateTimeout > 0) {
                        rtState.procEvtsUpdateTo = new UpdateProcessedEventsTimeoutObject(rtState, updateTimeout);

                        spi.getSpiContext().addTimeoutObject(rtState.procEvtsUpdateTo);
                    }
                }
            }
        }
    }

    /**
     * @param rtState Runtime state.
     * @param procEvtsUpdateTo Timeout object.
     * @throws Exception If failed.
     */
    private void updateProcessedEventsOnTimeout(ZkRuntimeState rtState, ZkTimeoutObject procEvtsUpdateTo)
        throws Exception
    {
        synchronized (stateMux) {
            if (rtState.procEvtsUpdateTo == procEvtsUpdateTo && rtState.locNodeInfo.needUpdate) {
                if (log.isDebugEnabled())
                    log.debug("Update processed events on timeout: " + rtState.locNodeInfo.lastProcEvt);

                updateProcessedEvents(rtState);
            }
        }
    }

    /**
     * @param rtState Runtime state.
     * @throws Exception If failed.
     */
    private void updateProcessedEvents(ZkRuntimeState rtState) throws Exception {
        try {
            rtState.zkClient.setData(rtState.locNodeZkPath, marshalZip(rtState.locNodeInfo), -1);

            rtState.locNodeInfo.needUpdate = false;
        }
        catch (KeeperException.NoNodeException e) {
            // Possible if node is forcible failed.
            if (log.isDebugEnabled())
                log.debug("Failed to update processed events, no node: " + rtState.locNodeInfo.lastProcEvt);
        }
    }

    /**
     * @param node Node.
     * @param joinedEvtData Joined node information.
     * @throws Exception If failed.
     */
    private void readAndInitSecuritySubject(ZookeeperClusterNode node, ZkJoinedNodeEvtData joinedEvtData) throws Exception {
        if (joinedEvtData.secSubjPartCnt > 0) {
            byte[] zipBytes = readMultipleParts(rtState.zkClient,
                zkPaths.joinEventSecuritySubjectPath(joinedEvtData.topVer),
                joinedEvtData.secSubjPartCnt);

            setNodeSecuritySubject(node, zipBytes);
        }
    }

    /**
     * @param evtsData Events data.
     * @param evtData Local join event data.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processLocalJoin(ZkDiscoveryEventsData evtsData,
        ZkJoinedNodeEvtData joinedEvtData,
        ZkDiscoveryNodeJoinEventData evtData)
        throws Exception
    {
        synchronized (stateMux) {
            if (connState == ConnectionState.STOPPED)
                return;

            if (rtState.joinTo != null) {
                spi.getSpiContext().removeTimeoutObject(rtState.joinTo);

                rtState.joinTo.cancelled = true;
                rtState.joinTo = null;
            }

            spi.getSpiContext().removeTimeoutObject(rtState.joinErrTo);

            if (log.isInfoEnabled())
                log.info("Local join event data: " + joinedEvtData + ']');

            String path = zkPaths.joinEventDataPathForJoined(evtData.eventId());

            byte[] dataForJoinedBytes = readMultipleParts(rtState.zkClient, path, evtData.dataForJoinedPartCnt);

            ZkJoinEventDataForJoined dataForJoined = unmarshalZip(dataForJoinedBytes);

            rtState.gridStartTime = evtsData.clusterStartTime;

            locNode.internalId(joinedEvtData.joinedInternalId);
            locNode.order(joinedEvtData.topVer);

            readAndInitSecuritySubject(locNode, joinedEvtData);

            byte[] discoDataBytes = dataForJoined.discoveryDataForNode(locNode.order());

            Map<Integer, Serializable> commonDiscoData =
                marsh.unmarshal(discoDataBytes, U.resolveClassLoader(spi.ignite().configuration()));

            DiscoveryDataBag dataBag = new DiscoveryDataBag(locNode.id(), locNode.isClient());

            dataBag.commonData(commonDiscoData);

            exchange.onExchange(dataBag);

            List<ZookeeperClusterNode> allNodes = dataForJoined.topology();

            for (int i = 0; i < allNodes.size(); i++) {
                ZookeeperClusterNode node = allNodes.get(i);

                // Need filter since ZkJoinEventDataForJoined contains single topology snapshot for all joined nodes.
                if (node.order() >= locNode.order())
                    break;

                rtState.top.addNode(node);
            }

            rtState.top.addNode(locNode);

            final List<ClusterNode> topSnapshot = rtState.top.topologySnapshot();

            lsnr.onDiscovery(EVT_NODE_JOINED,
                joinedEvtData.topVer,
                locNode,
                topSnapshot,
                Collections.<Long, Collection<ClusterNode>>emptyMap(),
                null);

            if (rtState.prevJoined) {
                lsnr.onDiscovery(EVT_CLIENT_NODE_RECONNECTED,
                    joinedEvtData.topVer,
                    locNode,
                    topSnapshot,
                    Collections.<Long, Collection<ClusterNode>>emptyMap(),
                    null);

                U.quietAndWarn(log, "Client node was reconnected after it was already considered failed [locId=" + locNode.id() + ']');
            }

            rtState.joined = true;
        }

        joinFut.onDone();

        if (locNode.isClient())
            rtState.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, null, new CheckClientsStatusCallback(rtState));
    }

    /**
     * @param evtData Event daa.
     * @param msg Message.
     * @throws Exception If failed.
     */
    private void processInternalMessage(ZkDiscoveryCustomEventData evtData, ZkInternalMessage msg) throws Exception {
        if (msg instanceof ZkForceNodeFailMessage)
            processForceNodeFailMessage((ZkForceNodeFailMessage)msg, evtData);
        else if (msg instanceof ZkCommunicationErrorResolveStartMessage) {
            processCommunicationErrorResolveStartMessage(
                (ZkCommunicationErrorResolveStartMessage)msg,
                evtData);
        }
        else if (msg instanceof ZkCommunicationErrorResolveFinishMessage) {
            processCommunicationErrorResolveFinishMessage(
                (ZkCommunicationErrorResolveFinishMessage)msg);
        }
        else if (msg instanceof ZkNoServersMessage)
            processNoServersMessage((ZkNoServersMessage)msg);
    }

    /**
     * @param msg Message.
     * @throws Exception If failed.
     */
    private void processNoServersMessage(ZkNoServersMessage msg) throws Exception {
        assert locNode.isClient() : locNode;

        throw localNodeFail("All server nodes failed, client node disconnected " +
            "(received 'no-servers' message) [locId=" + locNode.id() + ']', true);
    }

    /**
     * @param msg Message.
     * @param evtData Event data.
     * @throws Exception If failed.
     */
    private void processForceNodeFailMessage(ZkForceNodeFailMessage msg, ZkDiscoveryCustomEventData evtData)
        throws Exception {
        ClusterNode creatorNode = rtState.top.nodesById.get(evtData.sndNodeId);

        ZookeeperClusterNode node = rtState.top.nodesByInternalId.get(msg.nodeInternalId);

        assert node != null : msg.nodeInternalId;

        if (msg.warning != null) {
            U.warn(log, "Received force EVT_NODE_FAILED event with warning [" +
                "nodeId=" + node.id() +
                ", msg=" + msg.warning +
                ", nodeInitiatedEvt=" + (creatorNode != null ? creatorNode : evtData.sndNodeId) + ']');
        }
        else {
            U.warn(log, "Received force EVT_NODE_FAILED event [" +
                "nodeId=" + node.id() +
                ", nodeInitiatedEvt=" + (creatorNode != null ? creatorNode : evtData.sndNodeId) + ']');
        }

        if (node.isLocal())
            throw localNodeFail("Received force EVT_NODE_FAILED event for local node.", true);
        else
            notifyNodeFail(node.internalId(), evtData.topologyVersion());
    }

    /**
     * @param msg Message.
     * @throws Exception If failed.
     */
    private void processCommunicationErrorResolveFinishMessage(ZkCommunicationErrorResolveFinishMessage msg)
        throws Exception
    {
        UUID futId = msg.futId;

        assert futId != null;

        if (log.isInfoEnabled())
            log.info("Received communication error resolve finish message [reqId=" + futId + ']');

        rtState.commErrProcNodes = null;

        ZkCommunicationErrorResolveResult res = msg.res;

        if (res == null)
            res = unmarshalZip(ZkDistributedCollectDataFuture.readResult(rtState.zkClient, zkPaths, futId));

        ZkCommunicationErrorProcessFuture fut = commErrProcFut.get();

        assert fut != null;

        Set<Long> failedNodes = null;

        if (res.err != null)
            U.error(log, "Communication error resolve failed: " + res.err, res.err);
        else {
            if (res.killedNodes != null) {
                failedNodes = U.newHashSet(res.killedNodes.size());

                for (int i = 0; i < res.killedNodes.size(); i++) {
                    long internalId = res.killedNodes.get(i);

                    if (internalId == locNode.internalId()) {
                        fut.onError(new IgniteCheckedException("Local node is forced to stop " +
                            "by communication error resolver"));

                        if (rtState.crd)
                            deleteAliveNodes(res.killedNodes);

                        throw localNodeFail("Local node is forced to stop by communication error resolver " +
                            "[nodeId=" + locNode.id() + ']', false);
                    }

                    ZookeeperClusterNode node = rtState.top.nodesByInternalId.get(internalId);

                    assert node != null : internalId;

                    failedNodes.add(node.order());
                }

                long topVer = msg.topVer;

                for (int i = 0; i < res.killedNodes.size(); i++) {
                    long nodeInternalId = res.killedNodes.get(i);

                    ClusterNode node = rtState.top.nodesByInternalId.get(nodeInternalId);

                    assert node != null : nodeInternalId;

                    if (log.isInfoEnabled())
                        log.info("Node stop is forced by communication error resolver [nodeId=" + node.id() + ']');

                    notifyNodeFail(nodeInternalId, ++topVer);
                }
            }
        }

        fut.onFinishResolve(failedNodes);

        if (rtState.crd)
            deleteAliveNodes(res.killedNodes);
    }

    /**
     * @param internalIds Nodes internal IDs.
     * @throws Exception If failed.
     */
    private void deleteAliveNodes(@Nullable GridLongList internalIds) throws Exception {
        if (internalIds == null)
            return;

        List<String> alives = rtState.zkClient.getChildren(zkPaths.aliveNodesDir);

        for (int i = 0; i < alives.size(); i++) {
            String alive = alives.get(i);

            if (internalIds.contains(ZkIgnitePaths.aliveInternalId(alive)))
                rtState.zkClient.deleteIfExistsAsync(zkPaths.aliveNodesDir + "/" + alive);
        }
    }

    /**
     * @param msg Message.
     * @param evtData Event data.
     * @throws Exception If failed.
     */
    private void processCommunicationErrorResolveStartMessage(ZkCommunicationErrorResolveStartMessage msg,
        ZkDiscoveryCustomEventData evtData) throws Exception {
        ZkCommunicationErrorProcessFuture fut;

        for (;;) {
            fut = commErrProcFut.get();

            if (fut == null || fut.isDone()) {
                ZkCommunicationErrorProcessFuture newFut =
                    ZkCommunicationErrorProcessFuture.createOnStartResolveRequest(this);

                if (commErrProcFut.compareAndSet(fut, newFut))
                    fut = newFut;
                else
                    fut = commErrProcFut.get();
            }

            if (fut.onStartResolveRequest(evtData.topologyVersion()))
                break;
            else {
                try {
                    fut.get();
                }
                catch (Exception e) {
                    U.warn(log, "Previous communication error process future failed: " + e);
                }
            }
        }

        if (log.isInfoEnabled()) {
            log.info("Received communication error resolve request [reqId=" + msg.id +
                ", topVer=" + rtState.top.topologySnapshot() + ']');
        }

        assert !fut.isDone() : fut;

        final String futPath = zkPaths.distributedFutureBasePath(msg.id);
        final ZkCommunicationErrorProcessFuture fut0 = fut;

        rtState.commErrProcNodes = rtState.top.topologySnapshot();

        if (rtState.crd) {
            ZkDistributedCollectDataFuture nodeResFut = collectCommunicationStatusFuture(msg.id);

            fut.nodeResultCollectFuture(nodeResFut);
        }

        runInWorkerThread(new ZkRunnable(rtState, this) {
            @Override protected void run0() throws Exception {
                fut0.checkConnection(rtState, futPath, rtState.commErrProcNodes);
            }
        });
    }

    /**
     * @param futId Future ID.
     * @return Future.
     * @throws Exception If failed.
     */
    private ZkDistributedCollectDataFuture collectCommunicationStatusFuture(UUID futId) throws Exception {
        return new ZkDistributedCollectDataFuture(this, rtState, zkPaths.distributedFutureBasePath(futId),
            new Callable<Void>() {
                @Override public Void call() throws Exception {
                    // Future is completed from ZK event thread.
                    onCommunicationErrorResolveStatusReceived(rtState);

                    return null;
                }
            }
        );
    }

    /**
     * @param rtState Runtime state.
     * @throws Exception If failed.
     */
    private void onCommunicationErrorResolveStatusReceived(final ZkRuntimeState rtState) throws Exception {
        ZkDiscoveryEventsData evtsData = rtState.evtsData;

        UUID futId = evtsData.communicationErrorResolveFutureId();

        if (log.isInfoEnabled())
            log.info("Received communication status from all nodes [reqId=" + futId + ']');

        assert futId != null;

        String futPath = zkPaths.distributedFutureBasePath(futId);

        List<ClusterNode> initialNodes = rtState.commErrProcNodes;

        assert initialNodes != null;

        rtState.commErrProcNodes = null;

        List<ClusterNode> topSnapshot = rtState.top.topologySnapshot();

        Map<UUID, BitSet> nodesRes = U.newHashMap(topSnapshot.size());

        Exception err = null;

        for (ClusterNode node : topSnapshot) {
            byte[] stateBytes = ZkDistributedCollectDataFuture.readNodeResult(futPath,
                rtState.zkClient,
                node.order());

            ZkCommunicationErrorNodeState nodeState = unmarshalZip(stateBytes);

            if (nodeState.err != null) {
                if (err == null)
                    err = new Exception("Failed to resolve communication error.");

                err.addSuppressed(nodeState.err);
            }
            else {
                assert nodeState.commState != null;

                nodesRes.put(node.id(), nodeState.commState);
            }
        }

        long topVer = evtsData.topVer;

        GridLongList killedNodesList = null;

        if (err == null) {
            boolean fullyConnected = true;

            for (Map.Entry<UUID, BitSet> e : nodesRes.entrySet()) {
                if (!checkFullyConnected(e.getValue(), initialNodes, rtState.top)) {
                    fullyConnected = false;

                    break;
                }
            }

            if (fullyConnected) {
                if (log.isInfoEnabled()) {
                    log.info("Finish communication error resolve process automatically, there are no " +
                        "communication errors [reqId=" + futId + ']');
                }
            }
            else {
                CommunicationFailureResolver rslvr = spi.ignite().configuration().getCommunicationFailureResolver();

                if (rslvr != null) {
                    if (log.isInfoEnabled()) {
                        log.info("Call communication error resolver [reqId=" + futId +
                            ", rslvr=" + rslvr.getClass().getSimpleName() + ']');
                    }

                    ZkCommunicationFailureContext ctx = new ZkCommunicationFailureContext(
                        ((IgniteKernal)spi.ignite()).context().cache().context(),
                        topSnapshot,
                        initialNodes,
                        nodesRes);

                    try {
                        rslvr.resolve(ctx);

                        Set<ClusterNode> killedNodes = ctx.killedNodes();

                        if (killedNodes != null) {
                            if (log.isInfoEnabled()) {
                                log.info("Communication error resolver forced nodes stop [reqId=" + futId +
                                    ", killNodeCnt=" + killedNodes.size() +
                                    ", nodeIds=" + U.nodeIds(killedNodes) + ']');
                            }

                            killedNodesList = new GridLongList(killedNodes.size());

                            for (ClusterNode killedNode : killedNodes) {
                                killedNodesList.add(((ZookeeperClusterNode)killedNode).internalId());

                                evtsData.topVer++;
                            }
                        }
                    }
                    catch (Exception e) {
                        err = e;

                        U.error(log, "Failed to resolve communication error with configured resolver [reqId=" + futId + ']', e);
                    }
                }
            }
        }

        evtsData.communicationErrorResolveFutureId(null);

        ZkCommunicationErrorResolveResult res = new ZkCommunicationErrorResolveResult(killedNodesList, err);

        ZkCommunicationErrorResolveFinishMessage msg = new ZkCommunicationErrorResolveFinishMessage(futId, topVer);

        msg.res = res;

        ZkDistributedCollectDataFuture.saveResult(zkPaths.distributedFutureResultPath(futId),
            rtState.zkClient,
            marshalZip(res));

        evtsData.evtIdGen++;

        ZkDiscoveryCustomEventData evtData = new ZkDiscoveryCustomEventData(
            evtsData.evtIdGen,
            0L,
            topVer,
            locNode.id(),
            msg,
            null);

        evtData.resolvedMsg = msg;

        evtsData.addEvent(rtState.top.nodesByOrder.values(), evtData);

        saveAndProcessNewEvents();

        // Need re-check alive nodes in case join was delayed.
        rtState.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, rtState.watcher, rtState.watcher);
    }

    /**
     * @param commState Node communication state.
     * @param initialNodes Topology snapshot when communication error resolve started.
     * @param top Current topology.
     * @return {@code True} if node has connection to all alive nodes.
     */
    private boolean checkFullyConnected(BitSet commState, List<ClusterNode> initialNodes, ZkClusterNodes top) {
        int startIdx = 0;

        for (;;) {
            int idx = commState.nextClearBit(startIdx);

            if (idx >= initialNodes.size())
                return true;

            ClusterNode node = initialNodes.get(idx);

            if (top.nodesById.containsKey(node.id()))
                return false;

            startIdx = idx + 1;
        }
    }

    /**
     *
     */
    public void simulateNodeFailure() {
        ZkRuntimeState rtState = this.rtState;

        ZookeeperClient client = rtState.zkClient;

        client.deleteIfExistsAsync(zkPaths.aliveNodesDir);

        rtState.onCloseStart(new IgniteCheckedException("Simulate node failure error."));

        rtState.zkClient.close();
    }

    /**
     * @param evtData Event data.
     * @param msg Custom message.
     */
    @SuppressWarnings("unchecked")
    private void notifyCustomEvent(final ZkDiscoveryCustomEventData evtData, final DiscoverySpiCustomMessage msg) {
        assert !(msg instanceof ZkInternalMessage) : msg;

        if (log.isDebugEnabled())
            log.debug(" [topVer=" + evtData.topologyVersion() + ", msg=" + msg + ']');

        final ZookeeperClusterNode sndNode = rtState.top.nodesById.get(evtData.sndNodeId);

        assert sndNode != null : evtData;

        final List<ClusterNode> topSnapshot = rtState.top.topologySnapshot();

        lsnr.onDiscovery(
            DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT,
            evtData.topologyVersion(),
            sndNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            msg);
    }

    /**
     * @param joinedEvtData Event data.
     * @param joiningData Joining node data.
     */
    @SuppressWarnings("unchecked")
    private void notifyNodeJoin(ZkJoinedNodeEvtData joinedEvtData, ZkJoiningNodeData joiningData) {
        final ZookeeperClusterNode joinedNode = joiningData.node();

        joinedNode.order(joinedEvtData.topVer);
        joinedNode.internalId(joinedEvtData.joinedInternalId);

        rtState.top.addNode(joinedNode);

        final List<ClusterNode> topSnapshot = rtState.top.topologySnapshot();

        lsnr.onDiscovery(EVT_NODE_JOINED,
            joinedEvtData.topVer,
            joinedNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            null);
    }

    /**
     * @param evtData Event data.
     */
    private void notifyNodeFail(final ZkDiscoveryNodeFailEventData evtData) {
        notifyNodeFail(evtData.failedNodeInternalId(), evtData.topologyVersion());
    }

    /**
     * @param nodeInternalOrder Node order.
     * @param topVer Topology version.
     */
    private void notifyNodeFail(long nodeInternalOrder, long topVer) {
        final ZookeeperClusterNode failedNode = rtState.top.removeNode(nodeInternalOrder);

        assert failedNode != null && !failedNode.isLocal() : failedNode;

        PingFuture pingFut = pingFuts.get(failedNode.order());

        if (pingFut != null)
            pingFut.onDone(false);

        final List<ClusterNode> topSnapshot = rtState.top.topologySnapshot();

        lsnr.onDiscovery(EVT_NODE_FAILED,
            topVer,
            failedNode,
            topSnapshot,
            Collections.<Long, Collection<ClusterNode>>emptyMap(),
            null);

        stats.onNodeFailed();
    }

    /**
     * @param msg Message to log.
     * @param clientReconnect {@code True} if allow client reconnect.
     * @return Exception to be thrown.
     */
    private ZookeeperClientFailedException localNodeFail(String msg, boolean clientReconnect) {
        U.warn(log, msg);

//        if (locNode.isClient() && rtState.zkClient.connected()) {
//            String path = rtState.locNodeZkPath.substring(rtState.locNodeZkPath.lastIndexOf('/') + 1);
//
//            String joinDataPath = zkPaths.joiningNodeDataPath(locNode.id(), ZkIgnitePaths.aliveNodePrefixId(path));
//
//            try {
//                if (rtState.zkClient.existsNoRetry(joinDataPath))
//                    rtState.zkClient.deleteIfExistsNoRetry(joinDataPath, -1);
//            }
//            catch (Exception e) {
//                if (log.isDebugEnabled())
//                    log.debug("Failed to clean local node's join data on stop: " + e);
//            }
//        }

        if (rtState.zkClient.connected())
            rtState.zkClient.close();

        if (clientReconnect && clientReconnectEnabled) {
            assert locNode.isClient() : locNode;

            boolean reconnect = false;

            synchronized (stateMux) {
                if (connState == ConnectionState.STARTED) {
                    reconnect = true;

                    connState = ConnectionState.DISCONNECTED;

                    rtState.onCloseStart(disconnectError());
                }
            }

            if (reconnect) {
                UUID newId = UUID.randomUUID();

                U.quietAndWarn(log, "Client node will try to reconnect with new id [" +
                    "newId=" + newId +
                    ", prevId=" + locNode.id() +
                    ", locNode=" + locNode + ']');

                runInWorkerThread(new ReconnectClosure(newId));
            }
        }
        else {
            rtState.errForClose = new IgniteCheckedException(msg);

            notifySegmented();
        }

        // Stop any further processing.
        return new ZookeeperClientFailedException(msg);
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
                    case ZkDiscoveryEventData.ZK_EVT_NODE_JOIN: {
                        handleProcessedJoinEventAsync((ZkDiscoveryNodeJoinEventData)evtData);

                        break;
                    }

                    case ZkDiscoveryEventData.ZK_EVT_CUSTOM_EVT: {
                        DiscoverySpiCustomMessage ack = handleProcessedCustomEvent(ctx,
                            (ZkDiscoveryCustomEventData)evtData);

                        if (ack != null) {
                            ZkDiscoveryCustomEventData ackEvtData = createAckEvent(
                                ack,
                                (ZkDiscoveryCustomEventData)evtData);

                            if (newEvts == null)
                                newEvts = new ArrayList<>();

                            newEvts.add(ackEvtData);
                        }

                        break;
                    }

                    case ZkDiscoveryEventData.ZK_EVT_NODE_FAILED: {
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
     * @param ack Ack message.
     * @param origEvt Original custom event.
     * @return Event data.
     * @throws Exception If failed.
     */
    private ZkDiscoveryCustomEventData createAckEvent(
        DiscoverySpiCustomMessage ack,
        ZkDiscoveryCustomEventData origEvt) throws Exception {
        assert ack != null;

        rtState.evtsData.evtIdGen++;

        long evtId = rtState.evtsData.evtIdGen;

        byte[] ackBytes = marshalZip(ack);

        String path = zkPaths.ackEventDataPath(origEvt.eventId());

        if (log.isDebugEnabled())
            log.debug("Create ack event: " + path);

        // TODO ZK: https://issues.apache.org/jira/browse/IGNITE-8194
        rtState.zkClient.createIfNeeded(
            path,
            ackBytes,
            CreateMode.PERSISTENT);

        ZkDiscoveryCustomEventData ackEvtData = new ZkDiscoveryCustomEventData(
            evtId,
            origEvt.eventId(),
            rtState.evtsData.topVer, // Use actual topology version because topology version must be growing.
            locNode.id(),
            null,
            null);

        ackEvtData.resolvedMsg = ack;

        if (log.isDebugEnabled()) {
            log.debug("Generated CUSTOM event ack [origEvtId=" + origEvt.eventId() +
                ", evt=" + ackEvtData +
                ", evtSize=" + ackBytes.length +
                ", msg=" + ack + ']');
        }

        return ackEvtData;
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
    private void handleProcessedJoinEventAsync(ZkDiscoveryNodeJoinEventData evtData) throws Exception {
        if (log.isDebugEnabled())
            log.debug("All nodes processed node join [evtData=" + evtData + ']');

        for (int i = 0; i < evtData.joinedNodes.size(); i++) {
            ZkJoinedNodeEvtData joinedEvtData = evtData.joinedNodes.get(i);

            deleteJoiningNodeData(joinedEvtData.nodeId, joinedEvtData.joinDataPrefixId, joinedEvtData.joinDataPartCnt);

            if (joinedEvtData.secSubjPartCnt > 0) {
                deleteMultiplePartsAsync(rtState.zkClient,
                    zkPaths.joinEventSecuritySubjectPath(evtData.eventId()),
                    joinedEvtData.secSubjPartCnt);
            }
        }

        deleteDataForJoinedAsync(evtData);
    }

    /**
     * @param nodeId Node ID.
     * @param joinDataPrefixId Path prefix.
     * @param partCnt Parts count.
     */
    private void deleteJoiningNodeData(UUID nodeId, UUID joinDataPrefixId, int partCnt) {
        String evtDataPath = zkPaths.joiningNodeDataPath(nodeId, joinDataPrefixId);

        if (log.isDebugEnabled())
            log.debug("Delete joining node data [path=" + evtDataPath + ']');

        rtState.zkClient.deleteIfExistsAsync(evtDataPath);

        if (partCnt > 1)
            deleteMultiplePartsAsync(rtState.zkClient, evtDataPath + ":", partCnt);
    }

    /**
     * @param evtData Event data.
     */
    private void deleteDataForJoinedAsync(ZkDiscoveryNodeJoinEventData evtData) {
        String dataForJoinedPath = zkPaths.joinEventDataPathForJoined(evtData.eventId());

        if (log.isDebugEnabled())
            log.debug("Delete data for joined node [path=" + dataForJoinedPath + ']');

        deleteMultiplePartsAsync(rtState.zkClient, dataForJoinedPath, evtData.dataForJoinedPartCnt);
    }

    /**
     * @param ctx Context for log.
     * @param evtData Event data.
     * @return Ack message.
     * @throws Exception If failed.
     */
    @Nullable private DiscoverySpiCustomMessage handleProcessedCustomEvent(String ctx, ZkDiscoveryCustomEventData evtData)
        throws Exception {
        if (log.isDebugEnabled())
            log.debug("All nodes processed custom event [ctx=" + ctx + ", evtData=" + evtData + ']');

        if (!evtData.ackEvent()) {
            if (evtData.evtPath != null)
                deleteCustomEventDataAsync(rtState.zkClient, evtData.evtPath);
            else {
                if (evtData.resolvedMsg instanceof ZkCommunicationErrorResolveFinishMessage) {
                    UUID futId = ((ZkCommunicationErrorResolveFinishMessage)evtData.resolvedMsg).futId;

                    ZkDistributedCollectDataFuture.deleteFutureData(rtState.zkClient, zkPaths, futId, log);
                }
            }

            assert evtData.resolvedMsg != null || locNode.order() > evtData.topologyVersion() : evtData;

            if (evtData.resolvedMsg != null)
                return evtData.resolvedMsg.ackMessage();
        }
        else {
            String path = zkPaths.ackEventDataPath(evtData.origEvtId);

            if (log.isDebugEnabled())
                log.debug("Delete path: " + path);

            rtState.zkClient.deleteIfExistsAsync(path);
        }

        return null;
    }

    /**
     * @param c Closure to run.
     */
    void runInWorkerThread(Runnable c) {
        IgniteThreadPoolExecutor pool;

        synchronized (stateMux) {
            if (connState == ConnectionState.STOPPED) {
                LT.warn(log, "Do not run closure, node is stopped.");

                return;
            }

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
    public void stop() {
        stop0(new IgniteSpiException("Node stopped"));
    }

    /**
     * @param e Error.
     */
    private void stop0(Throwable e) {
        if (!stop.compareAndSet(false, true))
            return;

        ZkRuntimeState rtState = this.rtState;

        if (rtState.zkClient != null && rtState.locNodeZkPath != null && rtState.zkClient.connected()) {
            try {
                rtState.zkClient.deleteIfExistsNoRetry(rtState.locNodeZkPath, -1);
            }
            catch (Exception err) {
                if (log.isDebugEnabled())
                    log.debug("Failed to delete local node's znode on stop: " + err);
            }
        }

        IgniteCheckedException err = new IgniteCheckedException("Node stopped.");

        synchronized (stateMux) {
            connState = ConnectionState.STOPPED;

            rtState.onCloseStart(err);
        }

        IgniteUtils.shutdownNow(ZookeeperDiscoveryImpl.class, utilityPool, log);

        busyLock.block();

        busyLock.unblock();

        joinFut.onDone(e);

        ZookeeperClient zkClient = rtState.zkClient;

        if (zkClient != null)
            zkClient.close();

        finishFutures(err);
    }

    /**
     * @param err Error.
     */
    private void finishFutures(IgniteCheckedException err) {
        ZkCommunicationErrorProcessFuture commErrFut = commErrProcFut.get();

        if (commErrFut != null)
            commErrFut.onError(err);

        for (PingFuture fut : pingFuts.values())
            fut.onDone(err);
    }

    /**
     * @param busyLock Busy lock.
     * @param err Error.
     */
    void onFatalError(GridSpinBusyLock busyLock, Throwable err) {
        busyLock.leaveBusy();

        if (err instanceof ZookeeperClientFailedException)
            return; // Processed by ZookeeperClient listener.

        Ignite ignite = spi.ignite();

        if (stopping() || ignite == null)
            return;

        U.error(log, "Fatal error in ZookeeperDiscovery. " +
            "Stopping the node in order to prevent cluster wide instability.", err);

        stop0(err);

        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    IgnitionEx.stop(igniteInstanceName, true, true);

                    U.log(log, "Stopped the node successfully in response to fatal error in ZookeeperDiscoverySpi.");
                }
                catch (Throwable e) {
                    U.error(log, "Failed to stop the node successfully in response to fatal error in " +
                        "ZookeeperDiscoverySpi.", e);
                }
            }
        }, "node-stop-thread").start();

        if (err instanceof Error)
            throw (Error)err;
    }

    /**
     * @param zipBytes Zip-compressed bytes.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If failed.
     */
    private <T> T unmarshalZip(byte[] zipBytes) throws Exception {
        assert zipBytes != null && zipBytes.length > 0;

        InflaterInputStream in = new InflaterInputStream(new ByteArrayInputStream(zipBytes));

        return marsh.unmarshal(in, U.resolveClassLoader(spi.ignite().configuration()));
    }

    /**
     * @param obj Object.
     * @return Bytes.
     * @throws IgniteCheckedException If failed.
     */
    byte[] marshalZip(Object obj) throws IgniteCheckedException {
        assert obj != null;

        return zip(U.marshal(marsh, obj));
    }

    /**
     * @param bytes Bytes to compress.
     * @return Zip-compressed bytes.
     */
    private static byte[] zip(byte[] bytes) {
        Deflater deflater = new Deflater();

        deflater.setInput(bytes);
        deflater.finish();

        GridByteArrayOutputStream out = new GridByteArrayOutputStream(bytes.length);

        final byte[] buf = new byte[bytes.length];

        while (!deflater.finished()) {
            int cnt = deflater.deflate(buf);

            out.write(buf, 0, cnt);
        }

        return out.toByteArray();
    }

    /**
     * @param zipBytes Zip-compressed bytes.
     * @return Uncompressed bytes.
     * @throws DataFormatException If compressed data format is invalid.
     */
    public static byte[] unzip(byte[] zipBytes) throws DataFormatException {
        Inflater inflater = new Inflater();

        inflater.setInput(zipBytes);

        GridByteArrayOutputStream out = new GridByteArrayOutputStream(zipBytes.length * 2);

        final byte[] buf = new byte[zipBytes.length];

        while (!inflater.finished()) {
            int cnt = inflater.inflate(buf);

            out.write(buf, 0, cnt);
        }

        return out.toByteArray();
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
            finishFutures(disconnectError());

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
                    if (connState == ConnectionState.STARTED) {
                        connState = ConnectionState.DISCONNECTED;

                        rtState.onCloseStart(disconnectError());
                    }
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
    private class ZkWatcher extends ZkAbstractWatcher implements ZkRuntimeState.ZkWatcher {
        /**
         * @param rtState Runtime state.
         */
        ZkWatcher(ZkRuntimeState rtState) {
            super(rtState, ZookeeperDiscoveryImpl.this);
        }

        /** {@inheritDoc} */
        @Override public void process0(WatchedEvent evt) {
            if (evt.getType() == Event.EventType.NodeDataChanged) {
                if (evt.getPath().equals(zkPaths.evtsPath)) {
                    if (!rtState.crd)
                        rtState.zkClient.getDataAsync(evt.getPath(), this, this);
                }
                else
                    U.warn(log, "Received NodeDataChanged for unexpected path: " + evt.getPath());
            }
            else if (evt.getType() == Event.EventType.NodeChildrenChanged) {
                if (evt.getPath().equals(zkPaths.aliveNodesDir))
                    rtState.zkClient.getChildrenAsync(evt.getPath(), this, this);
                else if (evt.getPath().equals(zkPaths.customEvtsDir))
                    rtState.zkClient.getChildrenAsync(evt.getPath(), this, this);
                else
                    U.warn(log, "Received NodeChildrenChanged for unexpected path: " + evt.getPath());
            }
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            if (!onProcessStart())
                return;

            try {
                assert rc == 0 : KeeperException.Code.get(rc);

                if (path.equals(zkPaths.aliveNodesDir))
                    generateTopologyEvents(children);
                else if (path.equals(zkPaths.customEvtsDir))
                    generateCustomEvents(children);
                else
                    U.warn(log, "Children callback for unexpected path: " + path);

                onProcessEnd();
            }
            catch (Throwable e) {
                onProcessError(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (!onProcessStart())
                return;

            try {
                assert rc == 0 : KeeperException.Code.get(rc);

                if (path.equals(zkPaths.evtsPath)) {
                    if (!rtState.crd)
                        processNewEvents(data);
                }
                else
                    U.warn(log, "Data callback for unknown path: " + path);

                onProcessEnd();
            }
            catch (Throwable e) {
                onProcessError(e);
            }
        }
    }

    /**
     *
     */
    private class AliveNodeDataWatcher extends ZkAbstractWatcher implements ZkRuntimeState.ZkAliveNodeDataWatcher {
        /**
         * @param rtState Runtime state.
         */
        AliveNodeDataWatcher(ZkRuntimeState rtState) {
            super(rtState, ZookeeperDiscoveryImpl.this);
        }

        /** {@inheritDoc} */
        @Override public void process0(WatchedEvent evt) {
            if (evt.getType() == Event.EventType.NodeDataChanged)
                rtState.zkClient.getDataAsync(evt.getPath(), this, this);
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (!onProcessStart())
                return;

            try {
                assert rtState.crd;

                processResult0(rc, path, data);

                onProcessEnd();
            }
            catch (Throwable e) {
                onProcessError(e);
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

                Long nodeInternalId = ZkIgnitePaths.aliveInternalId(path);

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
    private abstract class PreviousNodeWatcher extends ZkAbstractWatcher implements AsyncCallback.StatCallback {
        /**
         * @param rtState Runtime state.
         */
        PreviousNodeWatcher(ZkRuntimeState rtState) {
            super(rtState, ZookeeperDiscoveryImpl.this);
        }

        /** {@inheritDoc} */
        @Override public void process0(WatchedEvent evt) {
            if (evt.getType() == Event.EventType.NodeDeleted)
                onPreviousNodeFail();
            else {
                if (evt.getType() != Event.EventType.None)
                    rtState.zkClient.existsAsync(evt.getPath(), this, this);
            }
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, Stat stat) {
            if (!onProcessStart())
                return;

            try {
                assert rc == 0 || rc == KeeperException.Code.NONODE.intValue() : KeeperException.Code.get(rc);

                if (rc == KeeperException.Code.NONODE.intValue() || stat == null)
                    onPreviousNodeFail();

                onProcessEnd();
            }
            catch (Throwable e) {
                onProcessError(e);
            }
        }

        /**
         *
         */
        abstract void onPreviousNodeFail();
    }

    /**
     *
     */
    private class ServerPreviousNodeWatcher extends PreviousNodeWatcher {
        /**
         * @param rtState Runtime state.
         */
        ServerPreviousNodeWatcher(ZkRuntimeState rtState) {
            super(rtState);

            assert !locNode.isClient() : locNode;
        }

        /** {@inheritDoc} */
        @Override void onPreviousNodeFail() {
            if (log.isInfoEnabled())
                log.info("Previous server node failed, check is node new coordinator [locId=" + locNode.id() + ']');

            rtState.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, null, new CheckCoordinatorCallback(rtState));
        }
    }

    /**
     *
     */
    private class ClientPreviousNodeWatcher extends PreviousNodeWatcher {
        /**
         * @param rtState Runtime state.
         */
        ClientPreviousNodeWatcher(ZkRuntimeState rtState) {
            super(rtState);

            assert locNode.isClient() : locNode;
        }

        /** {@inheritDoc} */
        @Override void onPreviousNodeFail() {
            if (log.isInfoEnabled())
                log.info("Watched node failed, check if there are alive servers [locId=" + locNode.id() + ']');

            rtState.zkClient.getChildrenAsync(zkPaths.aliveNodesDir, null, new CheckClientsStatusCallback(rtState));
        }
    }

    /**
     *
     */
    class CheckCoordinatorCallback extends ZkAbstractChildrenCallback {
        /**
         * @param rtState Runtime state.
         */
        CheckCoordinatorCallback(ZkRuntimeState rtState) {
            super(rtState, ZookeeperDiscoveryImpl.this);
        }

        /** {@inheritDoc} */
        @Override public void processResult0(int rc, String path, Object ctx, List<String> children, Stat stat)
            throws Exception
        {
            assert rc == 0 : KeeperException.Code.get(rc);

            checkIsCoordinator(children);
        }
    }

    /**
     *
     */
    class CheckClientsStatusCallback extends ZkAbstractChildrenCallback {
        /**
         * @param rtState Runtime state.
         */
        CheckClientsStatusCallback(ZkRuntimeState rtState) {
            super(rtState, ZookeeperDiscoveryImpl.this);
        }

        /** {@inheritDoc} */
        @Override void processResult0(int rc, String path, Object ctx, List<String> children, Stat stat)
            throws Exception
        {
            assert rc == 0 : KeeperException.Code.get(rc);

            checkClientsStatus(children);
        }
    }

    /**
     *
     */
    private class PingFuture extends GridFutureAdapter<Boolean> implements IgniteSpiTimeoutObject {
        /** */
        private final ZookeeperClusterNode node;

        /** */
        private final long endTime;

        /** */
        private final IgniteUuid id;

        /** */
        private final ZkRuntimeState rtState;

        /**
         * @param rtState Runtime state.
         * @param node Node.
         */
        PingFuture(ZkRuntimeState rtState, ZookeeperClusterNode node) {
            this.rtState = rtState;
            this.node = node;

            id = IgniteUuid.fromUuid(node.id());

            endTime = System.currentTimeMillis() + node.sessionTimeout() + 1000;
        };

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
            if (checkNodeAndState()) {
                runInWorkerThread(new ZkRunnable(rtState, ZookeeperDiscoveryImpl.this) {
                    @Override protected void run0() throws Exception {
                        if (checkNodeAndState()) {
                            try {
                                for (String path : rtState.zkClient.getChildren(zkPaths.aliveNodesDir)) {
                                    if (node.internalId() == ZkIgnitePaths.aliveInternalId(path)) {
                                        onDone(true);

                                        return;
                                    }
                                }

                                onDone(false);
                            }
                            catch (Exception e) {
                                onDone(e);

                                throw e;
                            }
                        }
                    }

                    @Override void onStartFailed() {
                        onDone(rtState.errForClose);
                    }
                });
            }
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                pingFuts.remove(node.order(), this);

                return true;
            }

            return false;
        }

        /**
         * @return {@code False} if future was completed.
         */
        boolean checkNodeAndState() {
            if (isDone())
                return false;

            Exception err = rtState.errForClose;

            if (err != null) {
                onDone(err);

                return false;
            }

            ConnectionState connState = ZookeeperDiscoveryImpl.this.connState;

            if (connState == ConnectionState.DISCONNECTED) {
                onDone(new IgniteClientDisconnectedException(null, "Client is disconnected."));

                return false;
            }
            else if (connState == ConnectionState.STOPPED) {
                onDone(new IgniteException("Node stopped."));

                return false;
            }

            if (node(node.id()) == null) {
                onDone(false);

                return false;
            }

            return true;
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

    /** */
    public UUID getCoordinator() {
        Map.Entry<Long, ZookeeperClusterNode> e = rtState.top.nodesByOrder.firstEntry();

        return e != null ? e.getValue().id() : null;
    }

    /** */
    public String getSpiState() {
        return rtState.zkClient.state();
    }

    /** */
    public String getZkSessionId() {
        if (rtState.zkClient != null && rtState.zkClient.zk() != null)
            return Long.toHexString(rtState.zkClient.zk().getSessionId());
        else
            return null;
    }
}
