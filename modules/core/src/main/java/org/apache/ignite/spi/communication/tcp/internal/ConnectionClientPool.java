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

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.ipc.shmem.IpcOutOfSystemResourcesException;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridShmemCommunicationClient;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.communication.tcp.AttributeNames;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.shmem.SHMemHandshakeClosure;
import org.apache.ignite.spi.discovery.IgniteDiscoveryThread;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.nonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DISABLED_CLIENT_PORT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.OUT_OF_RESOURCES_TCP_MSG;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.handshakeTimeoutException;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.nodeAddresses;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.usePairedConnections;

/**
 * Registry of client connections.
 */
public class ConnectionClientPool {
    /** Time threshold to log too long connection establish. */
    private static final int CONNECTION_ESTABLISH_THRESHOLD_MS = 100;

    /** Clients. */
    private final ConcurrentMap<UUID, GridCommunicationClient[]> clients = GridConcurrentFactory.newMap();

    /** Config. */
    private final TcpCommunicationConfiguration cfg;

    /** Attribute names. */
    private final AttributeNames attrs;

    /** Logger. */
    private final IgniteLogger log;

    /** Statistics. */
    @Nullable
    private volatile TcpCommunicationMetricsListener metricsLsnr;

    /** Local node supplier. */
    private final Supplier<ClusterNode> locNodeSupplier;

    /** Node getter. */
    private final Function<UUID, ClusterNode> nodeGetter;

    /** Message formatter supplier. */
    private final Supplier<MessageFormatter> msgFormatterSupplier;

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

    /**
     * @param cfg Config.
     * @param attrs Attributes.
     * @param log Logger.
     * @param metricsLsnr Metrics listener.
     * @param locNodeSupplier Local node supplier.
     * @param nodeGetter Node getter.
     * @param msgFormatterSupplier Message formatter supplier.
     * @param registry Registry.
     * @param tcpCommSpi Tcp communication spi.
     * @param clusterStateProvider Cluster state provider.
     * @param nioSrvWrapper Nio server wrapper.
     * @param igniteInstanceName Ignite instance name.
     */
    public ConnectionClientPool(
        TcpCommunicationConfiguration cfg,
        AttributeNames attrs,
        IgniteLogger log,
        TcpCommunicationMetricsListener metricsLsnr,
        Supplier<ClusterNode> locNodeSupplier,
        Function<UUID, ClusterNode> nodeGetter,
        Supplier<MessageFormatter> msgFormatterSupplier,
        WorkersRegistry registry,
        TcpCommunicationSpi tcpCommSpi,
        ClusterStateProvider clusterStateProvider,
        GridNioServerWrapper nioSrvWrapper,
        String igniteInstanceName
    ) {
        this.cfg = cfg;
        this.attrs = attrs;
        this.log = log;
        this.metricsLsnr = metricsLsnr;
        this.locNodeSupplier = locNodeSupplier;
        this.nodeGetter = nodeGetter;
        this.msgFormatterSupplier = msgFormatterSupplier;
        this.registry = registry;
        this.tcpCommSpi = tcpCommSpi;
        this.clusterStateProvider = clusterStateProvider;
        this.nioSrvWrapper = nioSrvWrapper;

        this.handshakeTimeoutExecutorService = newSingleThreadScheduledExecutor(
            new IgniteThreadFactory(igniteInstanceName, "handshake-timeout-client")
        );
    }

    /**
     *
     */
    public void stop() {
        this.stopping = true;

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
        assert node != null;
        assert (connIdx >= 0 && connIdx < cfg.connectionsPerNode())
            || !(cfg.usePairedConnections() && usePairedConnections(node, attrs.pairedConnection())) : connIdx;

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

            if (client.reserve())
                return client;
            else
                // Client has just been closed by idle worker. Help it and try again.
                removeNodeClient(nodeId, client);
        }
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

            triggerFut.listen(f -> {
                try {
                    fut0.onDone(f.get());
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

        Integer shmemPort = node.attribute(attrs.shmemPort());

        ClusterNode locNode = locNodeSupplier.get();

        if (locNode == null)
            throw new IgniteCheckedException("Failed to create NIO client (local node is stopping)");

        if (log.isDebugEnabled())
            log.debug("Creating NIO client to node: " + node);

        // If remote node has shared memory server enabled and has the same set of MACs
        // then we are likely to run on the same host and shared memory communication could be tried.
        if (shmemPort != null && U.sameMacs(locNode, node)) {
            try {
                // https://issues.apache.org/jira/browse/IGNITE-11126 Rework failure detection logic.
                GridCommunicationClient client = createShmemClient(
                    node,
                    connIdx,
                    shmemPort);

                if (log.isDebugEnabled())
                    log.debug("Shmem client created: " + client);

                return client;
            }
            catch (IgniteCheckedException e) {
                if (e.hasCause(IpcOutOfSystemResourcesException.class))
                    // Has cause or is itself the IpcOutOfSystemResourcesException.
                    LT.warn(log, OUT_OF_RESOURCES_TCP_MSG);
                else if (nodeGetter.apply(node.id()) != null)
                    LT.warn(log, e.getMessage());
                else if (log.isDebugEnabled()) {
                    log.debug("Failed to establish shared memory connection with local node (node has left): " +
                        node.id());
                }
            }
        }

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
     * @param port Port.
     * @param connIdx Connection index.
     * @return Client.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public GridCommunicationClient createShmemClient(
        ClusterNode node,
        int connIdx,
        Integer port
    ) throws IgniteCheckedException {
        int attempt = 1;

        int connectAttempts = 1;

        long connTimeout0 = cfg.connectionTimeout();

        IgniteSpiOperationTimeoutHelper timeoutHelper = new IgniteSpiOperationTimeoutHelper(tcpCommSpi, !node.isClient());

        while (true) {
            GridCommunicationClient client;

            try {
                client = new GridShmemCommunicationClient(
                    connIdx,
                    metricsLsnr.metricRegistry(),
                    port,
                    timeoutHelper.nextTimeoutChunk(cfg.connectionTimeout()),
                    log,
                    msgFormatterSupplier.get());
            }
            catch (IgniteCheckedException e) {
                if (timeoutHelper.checkFailureTimeoutReached(e))
                    throw e;

                // Reconnect for the second time, if connection is not established.
                if (connectAttempts < 2 && X.hasCause(e, ConnectException.class)) {
                    connectAttempts++;

                    continue;
                }

                throw e;
            }

            try {
                safeShmemHandshake(client, node.id(), timeoutHelper.nextTimeoutChunk(connTimeout0));
            }
            catch (IgniteSpiOperationTimeoutException e) {
                client.forceClose();

                if (cfg.failureDetectionTimeoutEnabled() && timeoutHelper.checkFailureTimeoutReached(e)) {
                    if (log.isDebugEnabled()) {
                        log.debug("Handshake timed out (failure threshold reached) [failureDetectionTimeout=" +
                            cfg.failureDetectionTimeout() + ", err=" + e.getMessage() + ", client=" + client + ']');
                    }

                    throw e;
                }

                assert !cfg.failureDetectionTimeoutEnabled();

                if (log.isDebugEnabled()) {
                    log.debug("Handshake timed out (will retry with increased timeout) [timeout=" + connTimeout0 +
                        ", err=" + e.getMessage() + ", client=" + client + ']');
                }

                if (attempt == cfg.reconCount() || connTimeout0 > cfg.maxConnectionTimeout()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Handshake timedout (will stop attempts to perform the handshake) " +
                            "[timeout=" + connTimeout0 + ", maxConnTimeout=" + cfg.maxConnectionTimeout() +
                            ", attempt=" + attempt + ", reconCnt=" + cfg.reconCount() +
                            ", err=" + e.getMessage() + ", client=" + client + ']');
                    }

                    throw e;
                }
                else {
                    attempt++;

                    connTimeout0 *= 2;

                    continue;
                }
            }
            catch (IgniteCheckedException | RuntimeException | Error e) {
                if (log.isDebugEnabled())
                    log.debug("Caught exception (will close client) [err=" + e.getMessage() + ", client=" + client + ']');

                client.forceClose();

                throw e;
            }

            return client;
        }
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
            assert !(cfg.usePairedConnections() && usePairedConnections(node, attrs.pairedConnection()));

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

                if (clients.putIfAbsent(node.id(), newClients) == null)
                    break;
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

    /**
     * @param nodeId Node ID.
     * @param rmvClient Client to remove.
     * @return {@code True} if client was removed.
     */
    public boolean removeNodeClient(UUID nodeId, GridCommunicationClient rmvClient) {
        if (log.isDebugEnabled())
            log.debug("The client was removed [nodeId=" + nodeId + ",  client=" + rmvClient.toString() + "].");

        for (; ; ) {
            GridCommunicationClient[] curClients = clients.get(nodeId);

            if (curClients == null
                || rmvClient.connectionIndex() >= curClients.length
                || curClients[rmvClient.connectionIndex()] != rmvClient)
                return false;

            GridCommunicationClient[] newClients = Arrays.copyOf(curClients, curClients.length);

            newClients[rmvClient.connectionIndex()] = null;

            if (clients.replace(nodeId, curClients, newClients))
                return true;
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

    /**
     * Performs handshake in timeout-safe way.
     *
     * @param client Client.
     * @param rmtNodeId Remote node.
     * @param timeout Timeout for handshake.
     * @throws IgniteCheckedException If handshake failed or wasn't completed withing timeout.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private void safeShmemHandshake(
        GridCommunicationClient client,
        UUID rmtNodeId,
        long timeout
    ) throws IgniteCheckedException {
        HandshakeTimeoutObject obj = new HandshakeTimeoutObject(client);

        handshakeTimeoutExecutorService.schedule(obj, timeout, TimeUnit.MILLISECONDS);

        try {
            client.doHandshake(new SHMemHandshakeClosure(log, rmtNodeId, clusterStateProvider, locNodeSupplier));
        }
        finally {
            if (!obj.cancel())
                throw handshakeTimeoutException();
        }
    }

    /**
     * @param metricsLsnr New statistics.
     */
    public void metricsListener(@Nullable TcpCommunicationMetricsListener metricsLsnr) {
        this.metricsLsnr = metricsLsnr;
    }
}
