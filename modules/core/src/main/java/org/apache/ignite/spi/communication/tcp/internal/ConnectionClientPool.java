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

package org.apache.ignite.spi.communication.tcp.internal;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteTooManyOpenFilesException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.AttributeNames;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.IgniteDiscoveryThread;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.nonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DISABLED_CLIENT_PORT;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.nodeAddresses;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.usePairedConnections;

/**
 * Registry of client connections.
 */
public class ConnectionClientPool {
    /** Time threshold to log too long connection establish. */
    private static final int CONNECTION_ESTABLISH_THRESHOLD_MS = 100;

    /** */
    public static final long NODE_METRICS_UPDATE_THRESHOLD = U.millisToNanos(200);

    /** */
    public static final String SHARED_METRICS_REGISTRY_NAME = metricName(TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME,
        "connection", "pool");

    /** */
    public static final String METRIC_POOL_SIZE_NAME = "poolSize";

    /** */
    public static final String METRIC_NAME_PAIRED_CONNS = "pairedConnections";

    /** */
    public static final String METRIC_NAME_ASYNC_CONNS = "asyncConnections";

    /** */
    private static final String NODE_METRICS_REGISTRY_NAME_PREFIX = metricName(SHARED_METRICS_REGISTRY_NAME, "node");

    /** */
    public static final String NODE_METRIC_NAME_CUR_CNT = "currentCnt";

    /** */
    public static final String NODE_METRIC_NAME_MSG_QUEUE_SIZE = "messagesQueueSize";

    /** */
    public static final String NODE_METRIC_NAME_REMOVED_CNT = "removedCnt";

    /** */
    public static final String NODE_METRIC_NAME_MAX_IDLE_TIME = "maxIdleTime";

    /** */
    public static final String NODE_METRIC_NAME_AVG_LIFE_TIME = "minLifeTime";

    /** */
    public static final String NODE_METRIC_NAME_ACQUIRING_THREADS_CNT = "acquiringThreadsCnt";

    /** Clients. */
    private final Map<UUID, GridCommunicationClient[]> clients
        = new ConcurrentHashMap<>(64, 0.75f, Math.max(16, Runtime.getRuntime().availableProcessors()));

    /** Metrics for each remote node. */
    private final Map<UUID, NodeMetrics> metrics;

    /** Config. */
    private final TcpCommunicationConfiguration cfg;

    /** Attribute names. */
    private final AttributeNames attrs;

    /** Logger. */
    private final IgniteLogger log;

    /** Local node supplier. */
    private final Supplier<ClusterNode> locNodeSupplier;

    /** Node getter. */
    private final Function<UUID, ClusterNode> nodeGetter;

    /** Workers registry. */
    private final WorkersRegistry registry;

    /** Tcp communication spi. */
    private final TcpCommunicationSpi tcpCommSpi;

    /** Cluster state provider. */
    private final ClusterStateProvider clusterStateProvider;

    /** Nio server wrapper. */
    private final GridNioServerWrapper nioSrvWrapper;

    /** Client connect futures. */
    private final ConcurrentMap<ConnectionKey, GridFutureAdapter<GridCommunicationClient>> clientFuts =
        GridConcurrentFactory.newMap();

    /** Stopping flag (set to {@code true} when SPI gets stopping signal). */
    private volatile boolean stopping = false;

    /** Scheduled executor service which closed the socket if handshake timeout is out. **/
    private final ScheduledExecutorService handshakeTimeoutExecutorService;

    /** Enable forcible node kill. */
    private boolean forcibleNodeKillEnabled = IgniteSystemProperties
        .getBoolean(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL);

    /** */
    @Nullable private final GridMetricManager metricsMgr;

    /** */
    private volatile AtomicBoolean asyncMetric;

    /**
     * @param cfg Config.
     * @param attrs Attributes.
     * @param log Logger.
     * @param locNodeSupplier Local node supplier.
     * @param nodeGetter Node getter.
     * @param registry Registry.
     * @param tcpCommSpi Tcp communication spi.
     * @param clusterStateProvider Cluster state provider.
     * @param nioSrvWrapper Nio server wrapper.
     * @param igniteInstanceName Ignite instance name.
     * @param metricsMgr Metrics manager. If {@code null}, metrics are not created.
     */
    public ConnectionClientPool(
        TcpCommunicationConfiguration cfg,
        AttributeNames attrs,
        IgniteLogger log,
        Supplier<ClusterNode> locNodeSupplier,
        Function<UUID, ClusterNode> nodeGetter,
        WorkersRegistry registry,
        TcpCommunicationSpi tcpCommSpi,
        ClusterStateProvider clusterStateProvider,
        GridNioServerWrapper nioSrvWrapper,
        String igniteInstanceName,
        @Nullable GridMetricManager metricsMgr
    ) {
        this.cfg = cfg;
        this.attrs = attrs;
        this.log = log;
        this.locNodeSupplier = locNodeSupplier;
        this.registry = registry;
        this.tcpCommSpi = tcpCommSpi;
        this.clusterStateProvider = clusterStateProvider;
        this.nioSrvWrapper = nioSrvWrapper;
        this.metricsMgr = metricsMgr;

        this.nodeGetter = new Function<>() {
            @Override public ClusterNode apply(UUID nodeId) {
                ClusterNode node = nodeGetter.apply(nodeId);

                if (node == null)
                    removeNodeMetrics(nodeId);

                return node;
            }
        };

        this.handshakeTimeoutExecutorService = newSingleThreadScheduledExecutor(
            new IgniteThreadFactory(igniteInstanceName, "handshake-timeout-client")
        );

        if (metricsMgr != null) {
            MetricRegistryImpl mreg = metricsMgr.registry(SHARED_METRICS_REGISTRY_NAME);

            mreg.register(METRIC_POOL_SIZE_NAME, () -> cfg.connectionsPerNode(), "Maximal connections number to a remote node.");
            mreg.register(METRIC_NAME_PAIRED_CONNS, () -> cfg.usePairedConnections(), "Paired connections flag.");

            metrics = new ConcurrentHashMap<>(64, 0.75f, Math.max(16, Runtime.getRuntime().availableProcessors()));
        }
        else
            metrics = null;
    }

    /**
     *
     */
    public void stop() {
        this.stopping = true;

        if (metricsMgr != null) {
            metricsMgr.remove(SHARED_METRICS_REGISTRY_NAME);

            clients.keySet().forEach(nodeId -> metricsMgr.remove(nodeMetricsRegName(nodeId)));
        }

        for (GridFutureAdapter<GridCommunicationClient> fut : clientFuts.values()) {
            if (fut instanceof ConnectionRequestFuture) {
                // There's no way it would be done by itself at this point.
                fut.onDone(new IgniteSpiException("SPI is being stopped."));
            }
        }

        handshakeTimeoutExecutorService.shutdown();
    }

    /**
     * Returns existing or just created client to node.
     *
     * @param node Node to which client should be open.
     * @param connIdx Connection index.
     * @return The existing or just created client.
     * @throws IgniteCheckedException Thrown if any exception occurs.
     */
    public GridCommunicationClient reserveClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
        NodeMetrics nodeMetrics = metrics.get(node.id());

        if (nodeMetrics != null)
            nodeMetrics.acquiringThreadsCnt.incrementAndGet();

        try {
            assert node != null;
            assert (connIdx >= 0 && connIdx < cfg.connectionsPerNode())
                || !(cfg.usePairedConnections() && usePairedConnections(node, attrs.pairedConnection()))
                || GridNioServerWrapper.isChannelConnIdx(connIdx) : "Wrong communication connection index: " + connIdx;

            if (locNodeSupplier.get().isClient()) {
                if (node.isClient()) {
                    if (DISABLED_CLIENT_PORT.equals(node.attribute(attrs.port())))
                        throw new IgniteSpiException("Cannot send message to the client node with no server socket opened.");
                }
            }

            UUID nodeId = node.id();

            if (log.isDebugEnabled())
                log.debug("The node client is going to reserve a connection [nodeId=" + node.id() + ", connIdx=" + connIdx + "]");

            while (true) {
                GridCommunicationClient[] curClients = clients.get(nodeId);

                GridCommunicationClient client = curClients != null && connIdx < curClients.length ?
                    curClients[connIdx] : null;

                if (client == null) {
                    if (stopping)
                        throw new IgniteSpiException("Node is stopping.");

                    // Do not allow concurrent connects.
                    GridFutureAdapter<GridCommunicationClient> fut = new ConnectFuture();

                    ConnectionKey connKey = new ConnectionKey(nodeId, connIdx, -1);

                    GridFutureAdapter<GridCommunicationClient> oldFut = clientFuts.putIfAbsent(connKey, fut);

                    if (oldFut == null) {
                        try {
                            GridCommunicationClient[] curClients0 = clients.get(nodeId);

                            GridCommunicationClient client0 = curClients0 != null && connIdx < curClients0.length ?
                                curClients0[connIdx] : null;

                            if (client0 == null) {
                                client0 = createCommunicationClient(node, connIdx);

                                if (client0 != null) {
                                    addNodeClient(node, connIdx, client0);

                                    if (client0 instanceof GridTcpNioCommunicationClient) {
                                        GridTcpNioCommunicationClient tcpClient = ((GridTcpNioCommunicationClient)client0);

                                        if (tcpClient.session().closeTime() > 0 && removeNodeClient(nodeId, client0)) {
                                            if (log.isDebugEnabled()) {
                                                log.debug("Session was closed after client creation, will retry " +
                                                    "[node=" + node + ", client=" + client0 + ']');
                                            }

                                            client0 = null;
                                        }
                                    }
                                }
                                else {
                                    U.sleep(200);

                                    if (nodeGetter.apply(node.id()) == null)
                                        throw new ClusterTopologyCheckedException("Failed to send message " +
                                            "(node left topology): " + node);
                                }
                            }

                            fut.onDone(client0);
                        }
                        catch (NodeUnreachableException e) {
                            log.warning(e.getMessage());

                            fut = handleUnreachableNodeException(node, connIdx, fut, e);
                        }
                        catch (Throwable e) {
                            if (e instanceof NodeUnreachableException)
                                throw e;

                            fut.onDone(e);

                            if (e instanceof IgniteTooManyOpenFilesException)
                                throw e;

                            if (e instanceof Error)
                                throw (Error)e;
                        }
                        finally {
                            clientFuts.remove(connKey, fut);
                        }
                    }
                    else
                        fut = oldFut;

                    long clientReserveWaitTimeout = registry != null ? registry.getSystemWorkerBlockedTimeout() / 3
                        : cfg.connectionTimeout() / 3;

                    long currTimeout = System.currentTimeMillis();

                    // This cycle will eventually quit when future is completed by concurrent thread reserving client.
                    while (true) {
                        try {
                            client = fut.get(clientReserveWaitTimeout, TimeUnit.MILLISECONDS);

                            break;
                        }
                        catch (IgniteFutureTimeoutCheckedException ignored) {
                            currTimeout += clientReserveWaitTimeout;

                            if (log.isDebugEnabled()) {
                                log.debug(
                                    "Still waiting for reestablishing connection to node " +
                                        "[nodeId=" + node.id() + ", waitingTime=" + currTimeout + "ms]"
                                );
                            }

                            if (registry != null) {
                                GridWorker wrkr = registry.worker(Thread.currentThread().getName());

                                if (wrkr != null)
                                    wrkr.updateHeartbeat();
                            }
                        }
                    }

                    if (client == null) {
                        if (clusterStateProvider.isLocalNodeDisconnected())
                            throw new IgniteCheckedException("Unable to create TCP client due to local node disconnecting.");
                        else
                            continue;
                    }

                    if (nodeGetter.apply(nodeId) == null) {
                        if (removeNodeClient(nodeId, client))
                            client.forceClose();

                        throw new IgniteSpiException("Destination node is not in topology: " + node.id());
                    }
                }

                assert connIdx == client.connectionIndex() : client;

                if (client.reserve()) {
                    if (metricsMgr != null)
                        updateClientAcquiredMetric(client);

                    return client;
                }
                else
                    // Client has just been closed by idle worker. Help it and try again.
                    removeNodeClient(nodeId, client);
            }
        }
        finally {
            if (nodeMetrics != null)
                nodeMetrics.acquiringThreadsCnt.decrementAndGet();
        }
    }

    /** */
    private void updateClientAcquiredMetric(GridCommunicationClient client) {
        if (asyncMetric == null) {
            synchronized (metrics) {
                if (asyncMetric == null) {
                    MetricRegistryImpl mreg = metricsMgr.registry(SHARED_METRICS_REGISTRY_NAME);

                    // We assume that all the clients have the same async flag.
                    asyncMetric = new AtomicBoolean(client.async());

                    mreg.register(METRIC_NAME_ASYNC_CONNS, () -> asyncMetric.get(), "Asynchronous flag. If TRUE, " +
                        "connections put data in a queue (with some preprocessing) instead of immediate sending.");
                }
            }
        }
        else
            assert client.async() == asyncMetric.get();
    }

    /**
     * Handles {@link NodeUnreachableException}. This means that the method will try to trigger client itself to open connection.
     * The only possible way of doing this is to use {@link TcpCommunicationConfiguration#connectionRequestor()}'s trigger and wait.
     * Specifics of triggers implementation technically should be considered unknown, but for now it's not true and we
     * expect that {@link NodeUnreachableException} won't be thrown in {@link IgniteDiscoveryThread}.
     *
     * @param node Node to open connection to.
     * @param connIdx Connection index.
     * @param fut Current future for opening connection.
     * @param e Curent exception.
     * @return New future that will return the client or error. {@code null} client is possible if newly opened
     *      connection has been closed by idle worker, at least that's what documentation says.
     * @throws IgniteCheckedException If trigerring failed or trigger is not configured.
     */
    private GridFutureAdapter<GridCommunicationClient> handleUnreachableNodeException(
        ClusterNode node,
        int connIdx,
        GridFutureAdapter<GridCommunicationClient> fut,
        NodeUnreachableException e
    ) throws IgniteCheckedException {
        if (cfg.connectionRequestor() != null) {
            ConnectFuture fut0 = (ConnectFuture)fut;

            final ConnectionKey key = new ConnectionKey(node.id(), connIdx, -1);

            ConnectionRequestFuture triggerFut = new ConnectionRequestFuture();

            triggerFut.listen(() -> {
                try {
                    fut0.onDone(triggerFut.get());
                }
                catch (Throwable t) {
                    fut0.onDone(t);
                }
                finally {
                    clientFuts.remove(key, triggerFut);
                }
            });

            clientFuts.put(key, triggerFut);

            fut = triggerFut;

            try {
                cfg.connectionRequestor().request(node, connIdx);

                long failTimeout = cfg.failureDetectionTimeoutEnabled()
                    ? cfg.failureDetectionTimeout()
                    : cfg.connectionTimeout();

                fut.get(failTimeout);
            }
            catch (Throwable triggerException) {
                if (forcibleNodeKillEnabled
                    && node.isClient()
                    && triggerException instanceof IgniteFutureTimeoutCheckedException
                ) {
                    CommunicationTcpUtils.failNode(node, tcpCommSpi.getSpiContext(), triggerException, log);
                }

                IgniteSpiException spiE = new IgniteSpiException(e);

                spiE.addSuppressed(triggerException);

                String msg = "Failed to wait for establishing inverse communication connection from node " + node;

                log.warning(msg, spiE);

                fut.onDone(spiE);

                throw spiE;
            }
        }
        else {
            fut.onDone(e);

            throw new IgniteCheckedException(e);
        }

        return fut;
    }

    /**
     * @param node Node to create client for.
     * @param connIdx Connection index.
     * @return Client.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public GridCommunicationClient createCommunicationClient(ClusterNode node, int connIdx)
        throws IgniteCheckedException {
        assert node != null;

        ClusterNode locNode = locNodeSupplier.get();

        if (locNode == null)
            throw new IgniteCheckedException("Failed to create NIO client (local node is stopping)");

        if (log.isDebugEnabled())
            log.debug("Creating NIO client to node: " + node);

        final long start = System.currentTimeMillis();

        GridCommunicationClient client = nioSrvWrapper.createTcpClient(node, connIdx, true);

        final long time = System.currentTimeMillis() - start;

        if (time > CONNECTION_ESTABLISH_THRESHOLD_MS) {
            if (log.isInfoEnabled())
                log.info("TCP client created [client=" + clientString(client, node) + ", duration=" + time + "ms]");
        }
        else if (log.isDebugEnabled())
            log.debug("TCP client created [client=" + clientString(client, node) + ", duration=" + time + "ms]");

        return client;
    }

    /**
     * Returns the string representation of client with protection from null client value. If the client if null, string
     * representation is built from cluster node.
     *
     * @param client communication client
     * @param node cluster node to which the client tried to establish a connection
     * @return string representation of client
     * @throws IgniteCheckedException if failed
     */
    public String clientString(GridCommunicationClient client, ClusterNode node) throws IgniteCheckedException {
        if (client == null) {
            assert node != null;

            StringJoiner joiner = new StringJoiner(", ", "null, node addrs=[", "]");

            for (InetSocketAddress addr : nodeAddresses(node, cfg.filterReachableAddresses(), attrs, locNodeSupplier))
                joiner.add(addr.toString());

            return joiner.toString();
        }
        else
            return client.toString();
    }

    /**
     * @param node Node.
     * @param connIdx Connection index.
     * @param addClient Client to add.
     */
    public void addNodeClient(ClusterNode node, int connIdx, GridCommunicationClient addClient) {
        assert cfg.connectionsPerNode() > 0 : cfg.connectionsPerNode();
        assert connIdx == addClient.connectionIndex() : addClient;

        if (log.isDebugEnabled()) {
            log.debug(
                "The node client is going to create a connection " +
                    "[nodeId=" + node.id() + ", connIdx=" + connIdx + ", client=" + addClient + "]"
            );
        }

        if (connIdx >= cfg.connectionsPerNode()) {
            assert !(cfg.usePairedConnections() && usePairedConnections(node, attrs.pairedConnection()))
                || GridNioServerWrapper.isChannelConnIdx(connIdx) : "Wrong communication connection index: " + connIdx;

            return;
        }

        for (; ; ) {
            GridCommunicationClient[] curClients = clients.get(node.id());

            assert curClients == null || curClients[connIdx] == null : "Client already created [node=" + node.id() +
                ", connIdx=" + connIdx +
                ", client=" + addClient +
                ", oldClient=" + curClients[connIdx] + ']';

            GridCommunicationClient[] newClients;

            if (curClients == null) {
                newClients = new GridCommunicationClient[cfg.connectionsPerNode()];
                newClients[connIdx] = addClient;

                if (clients.putIfAbsent(node.id(), newClients) == null) {
                    createNodeMetrics(node);

                    break;
                }
            }
            else {
                newClients = curClients.clone();
                newClients[connIdx] = addClient;

                if (log.isDebugEnabled())
                    log.debug("The node client was replaced [nodeId=" + node.id() + ", connIdx=" + connIdx + ", client=" + addClient + "]");

                if (clients.replace(node.id(), curClients, newClients))
                    break;
            }
        }
    }

    /** */
    private void createNodeMetrics(ClusterNode node) {
        if (metricsMgr == null)
            return;

        MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(node.id()));

        assert !mreg.iterator().hasNext() : "Node connection pools metrics aren't empty.";

        mreg.register(NODE_METRIC_NAME_CUR_CNT, () -> updatedNodeMetrics(node.id()).connsCnt,
            "Number of current connections to the remote node.");

        mreg.register(NODE_METRIC_NAME_MSG_QUEUE_SIZE, () -> updatedNodeMetrics(node.id()).msgsQueueSize,
            "Overal number of pending messages to the remote node.");

        mreg.register(NODE_METRIC_NAME_MAX_IDLE_TIME, () -> updatedNodeMetrics(node.id()).maxIdleTime,
            "Maximal idle time of sending or receiving data in milliseconds.");

        mreg.register(NODE_METRIC_NAME_AVG_LIFE_TIME, () -> updatedNodeMetrics(node.id()).avgLifetime,
            "Average connection lifetime in milliseconds.");

        mreg.register(NODE_METRIC_NAME_REMOVED_CNT, () -> updatedNodeMetrics(node.id()).removedConnectionsCnt.get(),
            "Total number of removed connections.");

        mreg.register(NODE_METRIC_NAME_ACQUIRING_THREADS_CNT, () -> updatedNodeMetrics(node.id()).acquiringThreadsCnt.get(),
            "Number of threads currently acquiring a connection.");

        // Create metrics record.
        updatedNodeMetrics(node.id());
    }

    /** */
    private NodeMetrics updatedNodeMetrics(UUID nodeId) {
        long nowNanos = System.nanoTime();

        NodeMetrics res = metrics.get(nodeId);

        if (res == null || (nowNanos - res.updateTs > NODE_METRICS_UPDATE_THRESHOLD && res.canUpdate())) {
            GridCommunicationClient[] nodeClients = clients.get(nodeId);

            if (nodeClients != null) {
                long nowMillis = U.currentTimeMillis();

                res = new NodeMetrics(res);

                long avgLifetime = 0;
                long maxIdleTime = 0;

                for (GridCommunicationClient nodeClient : nodeClients) {
                    if (nodeClient == null)
                        continue;

                    ++res.connsCnt;

                    avgLifetime += nowMillis - nodeClient.creationTime();

                    long nodeIdleTime = nodeClient.getIdleTime();

                    if (nodeIdleTime > maxIdleTime)
                        maxIdleTime = nodeIdleTime;

                    res.msgsQueueSize += nodeClient.messagesQueueSize();
                }

                if (res.connsCnt != 0)
                    res.avgLifetime = avgLifetime / res.connsCnt;

                res.maxIdleTime = maxIdleTime;

                res.updateTs = System.nanoTime();

                NodeMetrics res0 = res;

                clients.compute(nodeId, (nodeId0, clients) -> {
                    if (clients == null)
                        metrics.remove(nodeId);
                    else
                        metrics.put(nodeId, res0);

                    return clients;
                });
            }
            else {
                metrics.remove(nodeId);

                res = null;
            }
        }

        return res == null ? NodeMetrics.EMPTY : res;
    }

    /** */
    public static String nodeMetricsRegName(UUID nodeId) {
        return metricName(NODE_METRICS_REGISTRY_NAME_PREFIX, nodeId.toString());
    }

    /**
     * @param nodeId Node ID.
     * @param rmvClient Client to remove.
     * @return {@code True} if client was removed.
     */
    public boolean removeNodeClient(UUID nodeId, GridCommunicationClient rmvClient) {
        for (; ; ) {
            GridCommunicationClient[] curClients = clients.get(nodeId);

            if (curClients == null
                || rmvClient.connectionIndex() >= curClients.length
                || curClients[rmvClient.connectionIndex()] != rmvClient)
                return false;

            GridCommunicationClient[] newClients = Arrays.copyOf(curClients, curClients.length);

            newClients[rmvClient.connectionIndex()] = null;

            if (clients.replace(nodeId, curClients, newClients)) {
                NodeMetrics nodeMetrics = metricsMgr != null ? metrics.get(nodeId) : null;

                if (nodeMetrics != null && nodeMetrics != NodeMetrics.EMPTY)
                    nodeMetrics.removedConnectionsCnt.addAndGet(1);

                if (log.isDebugEnabled())
                    log.debug("The client was removed [nodeId=" + nodeId + ",  client=" + rmvClient.toString() + "].");

                return true;
            }
        }
    }

    /**
     * Closing connections to node.
     * NOTE: It is recommended only for tests.
     *
     * @param nodeId Node for which to close connections.
     * @throws IgniteCheckedException If occurs.
     */
    public void forceCloseConnection(UUID nodeId) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("The node client connections were closed [nodeId=" + nodeId + "]");

        removeNodeMetrics(nodeId);

        GridCommunicationClient[] clients = this.clients.remove(nodeId);
        if (nonNull(clients)) {
            for (GridCommunicationClient client : clients)
                client.forceClose();
        }

        for (ConnectionKey connKey : clientFuts.keySet()) {
            if (!nodeId.equals(connKey.nodeId()))
                continue;

            GridFutureAdapter<GridCommunicationClient> fut = clientFuts.remove(connKey);
            if (nonNull(fut))
                Optional.ofNullable(fut.get()).ifPresent(GridCommunicationClient::forceClose);
        }
    }

    /**
     * @param nodeId Node id.
     */
    public void onNodeLeft(UUID nodeId) {
        removeNodeMetrics(nodeId);

        GridCommunicationClient[] clients0 = clients.remove(nodeId);

        if (clients0 != null) {
            for (GridCommunicationClient client : clients0) {
                if (client != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Forcing NIO client close since node has left [nodeId=" + nodeId +
                            ", client=" + client + ']');
                    }

                    client.forceClose();
                }
            }
        }
    }

    /** */
    private void removeNodeMetrics(UUID nodeId) {
        if (metricsMgr != null) {
            metricsMgr.remove(nodeMetricsRegName(nodeId));

            metrics.remove(nodeId);
        }
    }

    /**
     * @param id Id.
     */
    public GridCommunicationClient[] clientFor(UUID id) {
        return clients.get(id);
    }

    /**
     * Clients entries.
     */
    public Iterable<? extends Map.Entry<UUID, GridCommunicationClient[]>> entrySet() {
        return clients.entrySet();
    }

    /**
     * @param connKey Connection key.
     * @param fut Future.
     */
    public void removeFut(ConnectionKey connKey, GridFutureAdapter<GridCommunicationClient> fut) {
        clientFuts.remove(connKey, fut);
    }

    /**
     * @param connKey Connection key.
     */
    public GridFutureAdapter<GridCommunicationClient> getFut(ConnectionKey connKey) {
        return clientFuts.get(connKey);
    }

    /**
     * @param key Key.
     * @param fut Future.
     */
    public GridFutureAdapter<GridCommunicationClient> putIfAbsentFut(
        ConnectionKey key,
        GridFutureAdapter<GridCommunicationClient> fut
    ) {
        return clientFuts.putIfAbsent(key, fut);
    }

    /**
     * Close all connections of this instance.
     */
    public void forceClose() {
        for (GridCommunicationClient[] clients0 : clients.values()) {
            for (GridCommunicationClient client : clients0) {
                if (client != null)
                    client.forceClose();
            }
        }
    }

    /**
     * @param err Err.
     */
    public void completeFutures(IgniteClientDisconnectedCheckedException err) {
        for (GridFutureAdapter<GridCommunicationClient> clientFut : clientFuts.values())
            clientFut.onDone(err);
    }

    /** */
    private static final class NodeMetrics {
        /** */
        private static final NodeMetrics EMPTY = new NodeMetrics(null);

        /** */
        private volatile long updateTs = System.nanoTime();

        /** */
        private volatile boolean updatingFlag;

        /** */
        private int connsCnt;

        /** */
        private int msgsQueueSize;

        /** */
        private long maxIdleTime;

        /** */
        private long avgLifetime;

        /** */
        private final AtomicInteger removedConnectionsCnt;

        /** */
        private final AtomicInteger acquiringThreadsCnt;

        /** */
        private NodeMetrics(@Nullable NodeMetrics prev) {
            this.removedConnectionsCnt = prev == null ? new AtomicInteger() : prev.removedConnectionsCnt;
            this.acquiringThreadsCnt = prev == null ? new AtomicInteger() : prev.acquiringThreadsCnt;
            this.avgLifetime = prev == null ? 0 : prev.avgLifetime;
            this.maxIdleTime = prev == null ? 0 : prev.maxIdleTime;
        }

        /** */
        private boolean canUpdate() {
            if (updatingFlag)
                return false;

            synchronized (this) {
                if (updatingFlag)
                    return false;

                updatingFlag = true;

                return true;
            }
        }
    }
}
