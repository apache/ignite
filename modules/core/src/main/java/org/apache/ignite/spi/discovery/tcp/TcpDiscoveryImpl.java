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

package org.apache.ignite.spi.discovery.tcp;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.tracing.NoopTracing;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiThread;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRingLatencyCheckMessage;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_METRICS_QNT_WARN;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 *
 */
abstract class TcpDiscoveryImpl {
    /** Response OK. */
    protected static final int RES_OK = 1;

    /** Response CONTINUE JOIN. */
    protected static final int RES_CONTINUE_JOIN = 100;

    /** Response WAIT. */
    protected static final int RES_WAIT = 200;

    /** Response join impossible. */
    protected static final int RES_JOIN_IMPOSSIBLE = 255;

    /** How often the warning message should occur in logs to prevent log spam. */
    public static final long LOG_WARN_MSG_TIMEOUT = 60 * 60 * 1000L;

    /** */
    protected final TcpDiscoverySpi spi;

    /** */
    protected final IgniteLogger log;

    /** */
    protected volatile TcpDiscoveryNode locNode;

    /** Debug mode. */
    protected boolean debugMode;

    /** Debug messages history. */
    private int debugMsgHist = 512;

    /** Received messages. */
    protected ConcurrentLinkedDeque<String> debugLogQ;

    /** Logging a warning message when metrics quantity exceeded a specified number. */
    protected int METRICS_QNT_WARN = getInteger(IGNITE_DISCOVERY_METRICS_QNT_WARN, 500);

    /** */
    protected long endTimeMetricsSizeProcessWait = System.currentTimeMillis();

    /** */
    protected final ServerImpl.DebugLogger debugLog = new DebugLogger() {
        /** {@inheritDoc} */
        @Override public boolean isDebugEnabled() {
            return log.isDebugEnabled();
        }

        /** {@inheritDoc} */
        @Override public void debug(String msg) {
            log.debug(msg);
        }
    };

    /** */
    protected final ServerImpl.DebugLogger traceLog = new DebugLogger() {
        /** {@inheritDoc} */
        @Override public boolean isDebugEnabled() {
            return log.isTraceEnabled();
        }

        /** {@inheritDoc} */
        @Override public void debug(String msg) {
            log.trace(msg);
        }
    };

    /** Tracing. */
    protected Tracing tracing;

    /**
     * Upcasts collection type.
     *
     * @param c Initial collection.
     * @return Resulting collection.
     */
    protected static <T extends R, R> Collection<R> upcast(Collection<T> c) {
        A.notNull(c, "c");

        return (Collection<R>)c;
    }

    /**
     * @param spi Adapter.
     */
    TcpDiscoveryImpl(TcpDiscoverySpi spi) {
        this.spi = spi;

        log = spi.log;

        if (spi.ignite() instanceof IgniteEx)
            tracing = ((IgniteEx) spi.ignite()).context().tracing();
        else
            tracing = new NoopTracing();
    }

    /**
     * This method is intended for troubleshooting purposes only.
     *
     * @param debugMode {code True} to start SPI in debug mode.
     */
    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
    }

    /**
     * This method is intended for troubleshooting purposes only.
     *
     * @param debugMsgHist Message history log size.
     */
    public void setDebugMessageHistory(int debugMsgHist) {
        this.debugMsgHist = debugMsgHist;
    }

    /**
     * @param discoMsg Discovery message.
     * @param msg Message.
     */
    protected void debugLog(@Nullable TcpDiscoveryAbstractMessage discoMsg, String msg) {
        assert debugMode;

        String msg0 = new SimpleDateFormat("[HH:mm:ss,SSS]").format(new Date(System.currentTimeMillis())) +
            '[' + Thread.currentThread().getName() + "][" + getLocalNodeId() +
            "-" + locNode.internalOrder() + "] " +
            msg;

        debugLogQ.add(msg0);

        int delta = debugLogQ.size() - debugMsgHist;

        for (int i = 0; i < delta && debugLogQ.size() > debugMsgHist; i++)
            debugLogQ.poll();
    }

    /**
     * @return Local node ID.
     */
    public UUID getLocalNodeId() {
        return spi.locNode.id();
    }

    /**
     * @return Configured node ID (actual node ID can be different if client reconnects).
     */
    public UUID getConfiguredNodeId() {
        return spi.cfgNodeId;
    }

    /**
     * @param msg Error message.
     * @param e Exception.
     */
    protected void onException(String msg, Exception e) {
        spi.getExceptionRegistry().onException(msg, e);
    }

    /**
     * Called when a local node either received from or sent to a remote node a message.
     */
    protected void onMessageExchanged() {
        // No-op
    }

    /**
     * @param log Logger.
     */
    public abstract void dumpDebugInfo(IgniteLogger log);

    /**
     * @return SPI state string.
     */
    public abstract String getSpiState();

    /**
     * @return Message worker queue current size.
     */
    public abstract int getMessageWorkerQueueSize();

    /**
     * @return Coordinator ID.
     */
    public abstract UUID getCoordinator();

    /**
     * @return Collection of remote nodes.
     */
    public abstract Collection<ClusterNode> getRemoteNodes();

    /**
     * @param feature Feature to check.
     * @return {@code true} if all nodes support the given feature, {@code false} otherwise.
     */
    public abstract boolean allNodesSupport(IgniteFeatures feature);

    /**
     * @param nodeId Node id.
     * @return Node with given ID or {@code null} if node is not found.
     */
    @Nullable public abstract ClusterNode getNode(UUID nodeId);

    /**
     * @param nodeId Node id.
     * @return {@code true} if node alive, {@code false} otherwise.
     */
    public abstract boolean pingNode(UUID nodeId);

    /**
     * Tells discovery SPI to disconnect from topology.
     *
     * @throws IgniteSpiException If failed.
     */
    public abstract void disconnect() throws IgniteSpiException;

    /**
     * @param msg Message.
     * @throws IgniteException If failed.
     */
    public abstract void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException;

    /**
     * @param nodeId Node id.
     * @param warning Warning message to be shown on all nodes.
     */
    public abstract void failNode(UUID nodeId, @Nullable String warning);

    /**
     * Dumps ring structure to logger.
     *
     * @param log Logger.
     */
    public abstract void dumpRingStructure(IgniteLogger log);

    /**
     * Get current topology version.
     *
     * @return Current topology version.
     */
    public abstract long getCurrentTopologyVersion();

    /**
     * @param igniteInstanceName Ignite instance name.
     * @throws IgniteSpiException If failed.
     */
    public abstract void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException;

    /**
     * Will start TCP server if applicable and not started yet.
     *
     * @return Port this instance bound to.
     * @throws IgniteSpiException If failed.
     */
    public int boundPort() throws IgniteSpiException {
        return 0;
    }

    /**
     * @return connection check interval.
     */
    public long connectionCheckInterval() {
        return 0;
    }

    /**
     * @throws IgniteSpiException If failed.
     */
    public abstract void spiStop() throws IgniteSpiException;

    /**
     * @param spiCtx Spi context.
     * @throws IgniteSpiException If failed.
     */
    public abstract void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException;

    /**
     * @param t Thread.
     * @return Status as string.
     */
    protected static String threadStatus(Thread t) {
        if (t == null)
            return "N/A";

        return t.isAlive() ? "alive" : "dead";
    }

    /**
     * Leave cluster and try to join again.
     *
     * @throws IgniteSpiException If failed.
     */
    public abstract void reconnect() throws IgniteSpiException;

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * Simulates this node failure by stopping service threads. So, node will become
     * unresponsive.
     * <p>
     * This method is intended for test purposes only.
     */
    abstract void simulateNodeFailure();

    /**
     * FOR TEST PURPOSE ONLY!
     */
    public abstract void brakeConnection();

    /**
     * @param maxHops Maximum hops for {@link TcpDiscoveryRingLatencyCheckMessage}.
     */
    public abstract void checkRingLatency(int maxHops);

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     *
     * @return Worker threads.
     */
    protected abstract Collection<IgniteSpiThread> threads();

    /**
     * @param nodeId Node ID.
     * @param metrics Metrics.
     * @param cacheMetrics Cache metrics.
     * @param tsNanos Timestamp as returned by {@link System#nanoTime()}.
     */
    public abstract void updateMetrics(UUID nodeId,
        ClusterMetrics metrics,
        Map<Integer, CacheMetrics> cacheMetrics,
        long tsNanos);

    /**
     * @throws IgniteSpiException If failed.
     */
    protected final void registerLocalNodeAddress() throws IgniteSpiException {
        long spiJoinTimeout = spi.getJoinTimeout();

        // Make sure address registration succeeded.
        // ... but limit it if join timeout is configured.
        long startNanos = spiJoinTimeout > 0 ? System.nanoTime() : 0;

        while (true) {
            try {
                spi.ipFinder.initializeLocalAddresses(
                    U.resolveAddresses(spi.getAddressResolver(), locNode.socketAddresses()));

                // Success.
                break;
            }
            catch (IllegalStateException e) {
                throw new IgniteSpiException("Failed to register local node address with IP finder: " +
                    locNode.socketAddresses(), e);
            }
            catch (IgniteSpiException e) {
                LT.error(log, e, "Failed to register local node address in IP finder on start " +
                    "(retrying every " + spi.getReconnectDelay() + " ms; " +
                    "change 'reconnectDelay' to configure the frequency of retries).");
            };

            if (spiJoinTimeout > 0 && U.millisSinceNanos(startNanos) > spiJoinTimeout)
                throw new IgniteSpiException(
                    "Failed to register local addresses with IP finder within join timeout " +
                        "(make sure IP finder configuration is correct, and operating system firewalls are disabled " +
                        "on all host machines, or consider increasing 'joinTimeout' configuration property) " +
                        "[joinTimeout=" + spiJoinTimeout + ']');

            try {
                U.sleep(spi.getReconnectDelay());
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }
        }
    }

    /**
     * @param ackTimeout Acknowledgement timeout.
     * @return {@code True} if acknowledgement timeout is less or equal to
     * maximum acknowledgement timeout, {@code false} otherwise.
     */
    protected boolean checkAckTimeout(long ackTimeout) {
        if (ackTimeout > spi.getMaxAckTimeout()) {
            LT.warn(log, "Acknowledgement timeout is greater than maximum acknowledgement timeout " +
                "(consider increasing 'maxAckTimeout' configuration property) " +
                "[ackTimeout=" + ackTimeout + ", maxAckTimeout=" + spi.getMaxAckTimeout() + ']');

            return false;
        }

        return true;
    }

    /** */
    public void processMsgCacheMetrics(TcpDiscoveryMetricsUpdateMessage msg, long tsNanos) {
        for (Map.Entry<UUID, TcpDiscoveryMetricsUpdateMessage.MetricsSet> e : msg.metrics().entrySet()) {
            UUID nodeId = e.getKey();

            TcpDiscoveryMetricsUpdateMessage.MetricsSet metricsSet = e.getValue();

            Map<Integer, CacheMetrics> cacheMetrics = msg.hasCacheMetrics(nodeId) ?
                msg.cacheMetrics().get(nodeId) : Collections.emptyMap();

            if (endTimeMetricsSizeProcessWait <= U.currentTimeMillis()
                && cacheMetrics.size() >= METRICS_QNT_WARN)
            {
                log.warning("The Discovery message has metrics for " + cacheMetrics.size() + " caches.\n" +
                    "To prevent Discovery blocking use -DIGNITE_DISCOVERY_DISABLE_CACHE_METRICS_UPDATE=true option.");

                endTimeMetricsSizeProcessWait = U.currentTimeMillis() + LOG_WARN_MSG_TIMEOUT;
            }

            updateMetrics(nodeId, metricsSet.metrics(), cacheMetrics, tsNanos);

            for (T2<UUID, ClusterMetrics> t : metricsSet.clientMetrics())
                updateMetrics(t.get1(), t.get2(), cacheMetrics, tsNanos);
        }
    }

    /**
     * @param addrs Addresses.
     */
    protected static List<String> toOrderedList(Collection<InetSocketAddress> addrs) {
        List<String> res = new ArrayList<>(addrs.size());

        for (InetSocketAddress addr : addrs)
            res.add(addr.toString());

        Collections.sort(res);

        return res;
    }

    /**
     * @param msg Message.
     * @return Message logger.
     */
    protected final DebugLogger messageLogger(TcpDiscoveryAbstractMessage msg) {
        return msg.traceLogLevel() ? traceLog : debugLog;
    }

    /**
     *
     */
    interface DebugLogger {
        /**
         * @return {@code True} if debug logging is enabled.
         */
        boolean isDebugEnabled();

        /**
         * @param msg Message to log.
         */
        void debug(String msg);
    }
}
