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
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.AttributeNames;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.IgniteDiscoveryThread;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.nonNull;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;
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
    public static final long METRICS_UPDATE_THRESHOLD = U.millisToNanos(200);

    /** */
    public static final String SHARED_METRICS_REGISTRY_NAME = metricName(TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME,
        "connectionPool");

    /** */
    public static final String METRIC_NAME_POOL_SIZE = "maxConnectionsCnt";

    /** */
    public static final String METRIC_NAME_PAIRED_CONNS = "isPaired";

    /** */
    public static final String METRIC_NAME_ASYNC_CONNS = "isAsync";

    /** It is handy for a user to see consistent id of a problematic node. */
    public static final String METRIC_NAME_CONSIST_ID = "consistentId";

    /** */
    public static final String METRIC_NAME_CUR_CNT = "currentConnectionsCnt";

    /** */
    public static final String METRIC_NAME_MSG_QUEUE_SIZE = "outboundMessagesQueueSize";

    /** */
    public static final String METRIC_NAME_REMOVED_CNT = "removedConnectionsCnt";

    /** */
    public static final String METRIC_NAME_MAX_NET_IDLE_TIME = "maxNetworkIdleTime";

    /** */
    public static final String METRIC_NAME_AVG_LIFE_TIME = "avgConnectionLifetime";

    /** */
    public static final String METRIC_NAME_ACQUIRING_THREADS_CNT = "acquiringThreadsCnt";

    /** Clients. */
    private final ConcurrentMap<UUID, GridCommunicationClient[]> clients = GridConcurrentFactory.newMap();

    /** Metrics for each remote node. */
    private final Map<UUID, NodeConnectionMetrics> metrics;

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
    private final boolean forcibleNodeKillEnabled = IgniteSystemProperties
        .getBoolean(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL);

    /** */
    private final GridMetricManager metricsMgr;

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
     * @param metricsMgr Metrics manager. If {@code null}, no metrics are created.
     */
    public ConnectionClientPool(
        TcpCommunicationConfiguration cfg,
        AttributeNames attrs,
        IgniteLogger log,
        @Nullable TcpCommunicationMetricsListener metricsLsnr,
        Supplier<ClusterNode> locNodeSupplier,
        Function<UUID, ClusterNode> nodeGetter,
        Supplier<MessageFormatter> msgFormatterSupplier,
        WorkersRegistry registry,
        TcpCommunicationSpi tcpCommSpi,
        ClusterStateProvider clusterStateProvider,
        GridNioServerWrapper nioSrvWrapper,
        String igniteInstanceName,
        GridMetricManager metricsMgr
    ) {
        this.cfg = cfg;
        this.attrs = attrs;
        this.log = log;
        this.metricsLsnr = metricsLsnr;
        this.locNodeSupplier = locNodeSupplier;
        this.msgFormatterSupplier = msgFormatterSupplier;
        this.registry = registry;
        this.tcpCommSpi = tcpCommSpi;
        this.clusterStateProvider = clusterStateProvider;
        this.nioSrvWrapper = nioSrvWrapper;
        this.metricsMgr = metricsMgr;
        this.nodeGetter = nodeGetter;

        this.handshakeTimeoutExecutorService = newSingleThreadScheduledExecutor("handshake-timeout-client", igniteInstanceName);

        metrics = new ConcurrentHashMap<>(64, 0.75f, Math.max(16, Runtime.getRuntime().availableProcessors()));

        MetricRegistryImpl mreg = metricsMgr.registry(SHARED_METRICS_REGISTRY_NAME);

        mreg.register(METRIC_NAME_POOL_SIZE, () -> cfg.connectionsPerNode(), "Maximal connections number to a remote node.");
        mreg.register(METRIC_NAME_PAIRED_CONNS, () -> cfg.usePairedConnections(), "Paired connections flag.");
        mreg.register(
            METRIC_NAME_ASYNC_CONNS,
            () -> true, // Currently we have only one connection implementation that is ASYNC (see GridTcpNioCommunicationClient#async)
            "Asynchronous flag. If TRUE, connections put data in a queue (with some preprocessing) instead of immediate sending.");
    }

    /**
     *
     */
    public void stop() {
        this.stopping = true;

        metricsMgr.remove(SHARED_METRICS_REGISTRY_NAME);

        metrics.values().forEach(NodeConnectionMetrics::unregister);

        metrics.clear();

        for (GridFutureAdapter<GridCommunicationClient> fut : clientFuts.values()) {
            // First completion wins: if the creating thread completes the future first, this call has no effect.
            fut.onDone(new IgniteSpiException("SPI is being stopped."));
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
        NodeConnectionMetrics nodeMetrics = metrics.get(node.id());

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

            // Total budget for the whole reservation: the same overall timeout that bounds a connection
            // establishment, so that threads waiting on a connect future are released within that budget
            // instead of blocking indefinitely. The budget bounds only the time spent waiting for a
            // connection to be established; a client that already exists is returned regardless of the
            // budget. The wait for a peer-initiated (inverse) connection after a failed direct attempt is
            // not covered by this budget: it has its own bounded window, because the failed direct
            // attempts have already consumed it.
            long totalReserveTimeout =
                CommunicationTcpUtils.totalConnectionEstablishmentTimeout(cfg, clusterStateProvider, node);

            // Use a monotonic clock for the budget: the system wall clock can be adjusted, which would make
            // the elapsed-time based give-up check unreliable.
            long reserveStartNanos = System.nanoTime();

            while (true) {
                GridCommunicationClient[] curClients = clients.get(nodeId);

                GridCommunicationClient client = curClients != null && connIdx < curClients.length ?
                    curClients[connIdx] : null;

                if (client == null) {
                    // The reserve budget bounds waiting for a connection to be established, not the
                    // availability of an existing client: check it only when no client is available, so
                    // that a client established by a concurrent thread in the meantime is returned even
                    // if the budget has already elapsed.
                    if (reserveElapsedMillis(reserveStartNanos) >= totalReserveTimeout)
                        throw reserveTimeoutException(node, totalReserveTimeout);

                    if (stopping)
                        throw new IgniteSpiException("Node is stopping.");

                    // Do not allow concurrent connects.
                    GridFutureAdapter<GridCommunicationClient> fut = new ConnectFuture();

                    ConnectionKey connKey = new ConnectionKey(nodeId, connIdx, -1);

                    GridFutureAdapter<GridCommunicationClient> oldFut = clientFuts.putIfAbsent(connKey, fut);

                    // Client established by this thread itself. When the thread reserving a connection is also
                    // the one that creates it, it can return that client directly instead of waiting on the
                    // shared future. The establishment already respected the connection deadline, so re-entering
                    // the wait/budget loop below (meant for threads waiting on another thread's future) could only
                    // reject a valid client once the reservation budget has elapsed. The fast path is taken only
                    // when this thread won the completion race for the shared future; if stop() completed it
                    // first, the client is dropped and the error carried by the future is propagated.
                    GridCommunicationClient ownClient = null;

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

                            // Fast path only if this call actually completed the shared future with the own
                            // client. If stop() (or another completion) won the race, the future already
                            // carries that outcome and the client must not be published: remove and close it,
                            // then let the wait loop below propagate the pending error.
                            if (fut.onDone(client0))
                                ownClient = client0;
                            else if (client0 != null) {
                                if (removeNodeClient(nodeId, client0))
                                    client0.forceClose();
                            }
                        }
                        catch (NodeUnreachableException e) {
                            log.warning(e.getMessage());

                            fut = handleUnreachableNodeException(node, connIdx, fut, e);

                            // The method returns only if the inverse connection has been established within
                            // its own bounded wait window, so the returned future is already done: take the
                            // client directly, like the own-client fast path above, instead of re-entering
                            // the wait/budget loop below. That loop bounds threads waiting on another
                            // thread's in-progress connection; the reservation budget has already been
                            // consumed by the failed direct attempts, so re-entering it would reject a
                            // valid client.
                            ownClient = fut.get();
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

                    if (ownClient != null)
                        client = ownClient;

                    if (client == null) {
                        long perWaitTimeout = registry != null ?
                            registry.getSystemWorkerBlockedTimeout() / 3 : cfg.connectionTimeout() / 3;

                        long currTimeout = System.currentTimeMillis();

                        // This cycle will eventually quit when future is completed by concurrent thread reserving client.
                        while (true) {
                            // A concurrent thread may have established the client while this thread was
                            // waiting: return it instead of waiting further or giving up, even if the
                            // reserve budget has already elapsed — the budget bounds waiting for a
                            // connection, not the availability of an existing client.
                            GridCommunicationClient[] curClients0 = clients.get(nodeId);

                            GridCommunicationClient curClient = curClients0 != null && connIdx < curClients0.length ?
                                curClients0[connIdx] : null;

                            if (curClient != null) {
                                client = curClient;

                                break;
                            }

                            // Recompute the remaining budget on every iteration and cap the individual
                            // wait by it, so that the total reserve budget is enforced even when a single
                            // wait would overshoot it. When the per-wait timeout is not configured (0),
                            // e.g. because the system worker blocked timeout is set to a non-positive
                            // value, wait the full remaining budget instead of aborting before it has
                            // elapsed.
                            long remainingReserveBudget = totalReserveTimeout - reserveElapsedMillis(reserveStartNanos);

                            long clientReserveWaitTimeout = perWaitTimeout > 0 ?
                                Math.min(perWaitTimeout, remainingReserveBudget) : remainingReserveBudget;

                            if (clientReserveWaitTimeout <= 0)
                                throw reserveTimeoutException(node, totalReserveTimeout);

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
        finally {
            if (nodeMetrics != null)
                nodeMetrics.acquiringThreadsCnt.decrementAndGet();
        }
    }

    /**
     * @param startNanos Monotonic start timestamp from {@link System#nanoTime()}.
     * @return Elapsed time in milliseconds, computed from the monotonic clock only.
     */
    private static long reserveElapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    /**
     * @param node                Node to reserve a client to.
     * @param totalReserveTimeout Total reserve budget in milliseconds.
     * @return Exception that reports the exhausted reserve budget.
     */
    private static IgniteSpiException reserveTimeoutException(ClusterNode node, long totalReserveTimeout) {
        return new IgniteSpiException("Failed to reserve a client to node " + node + " within " +
            totalReserveTimeout + " ms");
    }

    /**
     * Handles {@link NodeUnreachableException}. This means that the method will try to trigger client itself to open connection.
     * The only possible way of doing this is to use {@link TcpCommunicationConfiguration#connectionRequestor()}'s trigger and wait.
     * Specifics of triggers implementation technically should be considered unknown, but for now it's not true and we
     * expect that {@link NodeUnreachableException} won't be thrown in {@link IgniteDiscoveryThread}.
     *
     * <p>The wait for the peer-initiated (inverse) connection is bounded by its own window (the failure
     * detection timeout, or the connection timeout when the failure detection timeout is disabled)
     * instead of the reservation budget of {@link #reserveClient(ClusterNode, int)}: the failed direct
     * connection attempts that produced the {@link NodeUnreachableException} have already consumed that
     * budget, so capping the wait by its remainder would suppress the inverse-connection protocol
     * altogether. The synchronous {@code connectionRequestor().request(node, connIdx)} call precedes the
     * bounded wait and its duration is not covered by that window either, so this path relies on the
     * requestor returning promptly ({@link ConnectionRequestor#request(ClusterNode, int)}): a requestor
     * that blocked there indefinitely would leave the triggering thread unreleased by the reservation
     * budget as well.</p>
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

                // The wait for the inverse connection gets its own bounded window (the failure
                // detection timeout, or the connection timeout when the failure detection timeout is
                // disabled) instead of the reservation budget, which the failed direct attempts have
                // already consumed. The request call above is not covered by this window.
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

            GridCommunicationClient[] newClients = curClients == null
                ? new GridCommunicationClient[cfg.connectionsPerNode()]
                : Arrays.copyOf(curClients, curClients.length);

            newClients[connIdx] = addClient;

            boolean success;

            if (curClients == null) {
                success = clients.putIfAbsent(node.id(), newClients) == null;

                if (success)
                    registerNodeMetrics(node);
            }
            else
                success = clients.replace(node.id(), curClients, newClients);

            if (success) {
                if (log.isDebugEnabled())
                    log.debug("New node client were added [nodeId=" + node.id() + ", connIdx=" + connIdx + ", client=" + addClient + "]");

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
        for (; ; ) {
            GridCommunicationClient[] curClients = clients.get(nodeId);

            if (curClients == null
                || rmvClient.connectionIndex() >= curClients.length
                || curClients[rmvClient.connectionIndex()] != rmvClient)
                return false;

            GridCommunicationClient[] newClients = Arrays.copyOf(curClients, curClients.length);

            newClients[rmvClient.connectionIndex()] = null;

            if (clients.replace(nodeId, curClients, newClients)) {
                NodeConnectionMetrics nodeMetrics = metrics.get(nodeId);

                if (nodeMetrics != null)
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

        unregisterNodeMetrics(nodeId);

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
    public void cleanupNodeMetrics() {
        Iterator<Map.Entry<UUID, NodeConnectionMetrics>> iter = metrics.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<UUID, NodeConnectionMetrics> entry = iter.next();

            if (nodeGetter.apply(entry.getKey()) == null) {
                entry.getValue().unregister();

                iter.remove();
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
     * @param metricsLsnr New statistics.
     */
    public void metricsListener(@Nullable TcpCommunicationMetricsListener metricsLsnr) {
        this.metricsLsnr = metricsLsnr;
    }

    /** */
    private void registerNodeMetrics(ClusterNode node) {
        metrics.put(node.id(), new NodeConnectionMetrics(node));
    }

    /** */
    void unregisterNodeMetrics(UUID nodeId) {
        NodeConnectionMetrics nodeMetrics = metrics.remove(nodeId);

        if (nodeMetrics != null)
            nodeMetrics.unregister();
    }

    /** */
    private class NodeConnectionMetrics {
        /** */
        private final MetricRegistryImpl registry;

        /** */
        private final ClusterNode node;

        /** */
        private final AtomicLong removedConnectionsCnt = new AtomicLong();

        /** */
        private final AtomicLong maxIdleTimeOut = new AtomicLong();

        /** */
        private final AtomicInteger acquiringThreadsCnt = new AtomicInteger();

        /** */
        NodeConnectionMetrics(ClusterNode node) {
            this.node = node;
            registry = createRegistry();
        }

        /** */
        void unregister() {
            metricsMgr.remove(registry.name());
        }

        /** */
        private int connectionsCount() {
            GridCommunicationClient[] nodeClients = clients.get(node.id());

            return nodeClients == null ? 0 : (int)Arrays.stream(nodeClients).filter(Objects::nonNull).count();
        }

        /** */
        private int pendingMessagesCount() {
            GridCommunicationClient[] nodeClients = clients.get(node.id());

            return nodeClients == null
                ? 0
                : Arrays.stream(nodeClients).filter(Objects::nonNull).mapToInt(GridCommunicationClient::messagesQueueSize).sum();
        }

        /** */
        private int clientTotalReservationCount() {
            return acquiringThreadsCnt.get();
        }

        /** */
        private long averageConnectionLifeTime() {
            GridCommunicationClient[] nodeClients = clients.get(node.id());

            if (nodeClients == null)
                return 0;

            long now = U.currentTimeMillis();
            int connCnt = 0;

            long totalLifetime = 0;

            for (GridCommunicationClient client : nodeClients) {
                if (client == null)
                    continue;

                ++connCnt;

                totalLifetime += (now - client.creationTime());
            }

            return connCnt != 0 ? totalLifetime / connCnt : 0;
        }

        /** */
        private long maxConnectionsIdleTime() {
            GridCommunicationClient[] nodeClients = clients.get(node.id());

            long max = nodeClients == null
                ? 0
                : Arrays.stream(nodeClients).filter(Objects::nonNull).mapToLong(GridCommunicationClient::getIdleTime).max().orElse(0);

            long cur;

            do {
                cur = maxIdleTimeOut.get();
            }
            while (cur < max && !maxIdleTimeOut.compareAndSet(cur, max));

            return maxIdleTimeOut.get();
        }

        /** */
        private long removedConnectionsCount() {
            return removedConnectionsCnt.get();
        }

        /** */
        private MetricRegistryImpl createRegistry() {
            MetricRegistryImpl mreg = metricsMgr.registry(metricName(SHARED_METRICS_REGISTRY_NAME, node.id().toString()));

            mreg.register(
                METRIC_NAME_CONSIST_ID,
                () -> node.consistentId().toString(),
                String.class,
                "Consistent id of the remote node as string.");
            mreg.register(
                METRIC_NAME_CUR_CNT,
                this::connectionsCount,
                "Number of current connections to the remote node.");
            mreg.register(
                METRIC_NAME_MSG_QUEUE_SIZE,
                this::pendingMessagesCount,
                "Overall number of pending messages to the remote node.");
            mreg.register(
                METRIC_NAME_MAX_NET_IDLE_TIME,
                this::maxConnectionsIdleTime,
                "Maximal idle time of physical sending or receiving data in milliseconds.");
            mreg.register(
                METRIC_NAME_AVG_LIFE_TIME,
                this::averageConnectionLifeTime,
                "Average connection lifetime in milliseconds.");
            mreg.register(
                METRIC_NAME_REMOVED_CNT,
                this::removedConnectionsCount,
                "Total number of removed connections.");
            mreg.register(
                METRIC_NAME_ACQUIRING_THREADS_CNT,
                this::clientTotalReservationCount,
                "Number of threads currently acquiring a connection.");

            return mreg;
        }
    }
}
