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

package org.apache.ignite.internal.client.impl.connection;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.net.ssl.SSLContext;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.client.GridClientCacheFlag;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientDataMetrics;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFuture;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.impl.GridClientDataMetricsAdapter;
import org.apache.ignite.internal.client.impl.GridClientFutureAdapter;
import org.apache.ignite.internal.client.impl.GridClientFutureCallback;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.jetbrains.annotations.Nullable;

/**
 * Facade for all possible network communications between client and server. Introduced to hide
 * protocol implementation (TCP, HTTP) from client code.
 */
public abstract class GridClientConnection {
    /** Topology */
    protected GridClientTopology top;

    /** Client id. */
    protected final UUID clientId;

    /** Server address this connection connected to */
    private InetSocketAddress srvAddr;

    /** SSL context to use if ssl is enabled. */
    private SSLContext sslCtx;

    /** Client credentials. */
    private SecurityCredentials cred;

    /** Reason why connection was closed. {@code null} means connection is still alive. */
    protected volatile GridClientConnectionCloseReason closeReason;

    /**
     * Creates a facade.
     *
     * @param clientId Client identifier.
     * @param srvAddr Server address this connection connected to.
     * @param sslCtx SSL context to use if SSL is enabled, {@code null} otherwise.
     * @param top Topology.
     * @param cred Client credentials.
     */
    protected GridClientConnection(UUID clientId, InetSocketAddress srvAddr, SSLContext sslCtx, GridClientTopology top,
        SecurityCredentials cred) {
        assert top != null;

        this.clientId = clientId;
        this.srvAddr = srvAddr;
        this.top = top;
        this.sslCtx = sslCtx;
        this.cred = cred;
    }

    /**
     * Closes connection facade.
     *
     * @param reason Why this connection should be closed.
     * @param waitCompletion If {@code true} this method will wait until all pending requests are handled.
     */
    abstract void close(GridClientConnectionCloseReason reason, boolean waitCompletion);

    /**
     * Closes connection facade if no requests are in progress.
     *
     * @param idleTimeout Idle timeout.
     * @return {@code True} if no requests were in progress and client was closed, {@code false} otherwise.
     */
    abstract boolean closeIfIdle(long idleTimeout);

    /**
     * Gets server address this connection connected to.
     *
     * @return Server address this connection connected to.
     */
    public InetSocketAddress serverAddress() {
        return srvAddr;
    }

    /**
     * Puts key-value pair into cache.
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return If value was actually put.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public <K, V> GridClientFutureAdapter<Boolean> cachePut(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        return cachePutAll(cacheName, Collections.singletonMap(key, val), flags, destNodeId);
    }

    /**
     * Gets entry from the cache for specified key.
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return Value.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public <K, V> GridClientFutureAdapter<V> cacheGet(String cacheName, final K key, Set<GridClientCacheFlag> flags,
        UUID destNodeId) throws GridClientConnectionResetException, GridClientClosedException {
        final GridClientFutureAdapter<Map<K, V>> res = cacheGetAll(cacheName, Collections.singleton(key), flags,
            destNodeId);

        return res.chain(new GridClientFutureCallback<Map<K, V>, V>() {
            @Override public V onComplete(GridClientFuture<Map<K, V>> fut) throws GridClientException {
                Map<K, V> map = fut.get();

                return F.firstValue(map);
            }
        });
    }

    /**
     * Removes entry from the cache for specified key.
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return Whether entry was actually removed.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K> GridClientFutureAdapter<Boolean> cacheRemove(String cacheName, K key,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Puts bundle of entries into cache.
     *
     * @param cacheName Cache name.
     * @param entries Entries.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return {@code True} if map contained more then one entry or if put succeeded in case of one entry,
     *      {@code false} otherwise
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K, V> GridClientFutureAdapter<Boolean> cachePutAll(String cacheName, Map<K, V> entries,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Gets bundle of entries for specified keys from the cache.
     *
     * @param cacheName Cache name.
     * @param keys Keys.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return Entries.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K, V> GridClientFutureAdapter<Map<K, V>> cacheGetAll(String cacheName, Collection<K> keys,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Removes bundle of entries for specified keys from the cache.
     *
     * @param cacheName Cache name.
     * @param keys Keys.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return Whether entries were actually removed
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K> GridClientFutureAdapter<Boolean> cacheRemoveAll(String cacheName, Collection<K> keys,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Replace key-value pair in cache if already exist.
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return Whether value was actually replaced.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K, V> GridClientFutureAdapter<Boolean> cacheReplace(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * <table>
     *     <tr><th>New value</th><th>Actual/old value</th><th>Behaviour</th></tr>
     *     <tr><td>null     </td><td>null   </td><td>Remove entry for key.</td></tr>
     *     <tr><td>newVal   </td><td>null   </td><td>Put newVal into cache if such key doesn't exist.</td></tr>
     *     <tr><td>null     </td><td>oldVal </td><td>Remove if actual value oldVal is equals to value in cache.</td></tr>
     *     <tr><td>newVal   </td><td>oldVal </td><td>Replace if actual value oldVal is equals to value in cache.</td></tr>
     * </table>
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param newVal Value 1.
     * @param oldVal Value 2.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return Whether new value was actually set.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K, V> GridClientFutureAdapter<Boolean> cacheCompareAndSet(String cacheName, K key, V newVal,
        V oldVal, Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Gets cache metrics for the key.
     *
     * @param cacheName Cache name.
     * @param destNodeId Destination node ID.
     * @return Metrics.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K> GridClientFutureAdapter<GridClientDataMetrics> cacheMetrics(String cacheName, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Append requested value to already cached one.
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value to append to the cached one.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return Whether new value was actually set.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K, V> GridClientFutureAdapter<Boolean> cacheAppend(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Prepend requested value to already cached one.
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value to prepend to the cached one.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return Whether new value was actually set.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K, V> GridClientFutureAdapter<Boolean> cachePrepend(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Execute task in the grid.
     *
     * @param taskName Task name.
     * @param arg Task argument.
     * @param destNodeId Destination node ID.
     * @param keepBinaries Keep binary flag.
     * @return Task execution result.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <R> GridClientFutureAdapter<R> execute(String taskName, Object arg, UUID destNodeId,
        boolean keepBinaries) throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Change grid global state.
     *
     * @param active Active.
     * @param destNodeId Destination node id.
     * @deprecated Use {@link #changeState(ClusterState, UUID)} instead.
     */
    @Deprecated
    public abstract GridClientFuture<?> changeState(boolean active, UUID destNodeId)
            throws GridClientClosedException, GridClientConnectionResetException;

    /**
     * Changes grid global state.
     *
     * @param state New cluster state.
     * @param destNodeId Destination node id.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract GridClientFuture<?> changeState(ClusterState state, UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException;

    /**
     * Get current grid state.
     *
     * @param destNodeId Destination node id.
     * @deprecated Use {@link #state(UUID)} instead.
     */
    @Deprecated
    public abstract GridClientFuture<Boolean> currentState(UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException;

    /**
     * Gets current grid global state.
     *
     * @param destNodeId Destination node id.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract GridClientFuture<ClusterState> state(UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException;

    /**
     * Get a cluster name.
     *
     * @param destNodeId Destination node id.
     * @return Future to get the cluster name.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract GridClientFuture<String> clusterName(UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException;

    /**
     * Gets node by node ID.
     *
     * @param id Node ID.
     * @param inclAttrs Whether to include attributes.
     * @param inclMetrics Whether to include metrics.
     * @param destNodeId Destination node ID.
     * @return Node.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract GridClientFuture<GridClientNode> node(UUID id, boolean inclAttrs, boolean inclMetrics,
        UUID destNodeId) throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Gets node by node IP.
     *
     * @param ipAddr IP address.
     * @param inclAttrs Whether to include attributes.
     * @param includeMetrics Whether to include metrics.
     * @param destNodeId Destination node ID.
     * @return Node.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract GridClientFuture<GridClientNode> node(String ipAddr, boolean inclAttrs,
        boolean includeMetrics, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Gets grid topology nodes.
     *
     * @param inclAttrs Whether to include attributes.
     * @param inclMetrics Whether to include metrics.
     * @param destNodeId Destination node ID.
     * @return Nodes.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract GridClientFuture<List<GridClientNode>> topology(boolean inclAttrs, boolean inclMetrics,
        @Nullable UUID destNodeId) throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Forwards a message in raw form to the connected node. This method supposed to be used only inside router.
     * The exact types of acceptable arguments and return values depend on connection implementation.
     *
     * @param body A raw message to send.
     * @return Future holding server's response.
     * @throws GridClientException If message forwarding failed.
     */
    public abstract GridClientFutureAdapter<?> forwardMessage(Object body) throws GridClientException;

    /**
     * @return {@code True} if connection is closed.
     */
    public boolean isClosed() {
        return closeReason != null;
    }

    /**
     * Gets SSLContext of this client connection.
     *
     * @return {@link SSLContext} instance.
     */
    protected SSLContext sslContext() {
        return sslCtx;
    }

    /**
     * Returns credentials for this client connection.
     *
     * @return Credentials.
     */
    protected SecurityCredentials credentials() {
        return cred;
    }

    /**
     * Safely gets long value by given key.
     *
     * @param map Map to get value from.
     * @param key Metrics name.
     * @return Value or -1, if not found.
     */
    protected long safeLong(Map<String, Number> map, String key) {
        Number val = map.get(key);

        if (val == null)
            return -1;

        return val.longValue();
    }

    /**
     * Safely gets double value by given key.
     *
     * @param map Map to get value from.
     * @param key Metrics name.
     * @return Value or -1, if not found.
     */
    protected double safeDouble(Map<String, Number> map, String key) {
        Number val = map.get(key);

        if (val == null)
            return -1;

        return val.doubleValue();
    }

    /**
     * Converts metrics map to metrics object.
     *
     * @param metricsMap Map to convert.
     * @return Metrics object.
     */
    protected GridClientDataMetrics metricsMapToMetrics(Map<String, Number> metricsMap) {
        GridClientDataMetricsAdapter metrics = new GridClientDataMetricsAdapter();

        metrics.createTime(safeLong(metricsMap, "createTime"));
        metrics.readTime(safeLong(metricsMap, "readTime"));
        metrics.writeTime(safeLong(metricsMap, "writeTime"));
        metrics.reads((int)safeLong(metricsMap, "reads"));
        metrics.writes((int)safeLong(metricsMap, "writes"));
        metrics.hits((int)safeLong(metricsMap, "hits"));
        metrics.misses((int)safeLong(metricsMap, "misses"));

        return metrics;
    }

    /**
     * Check if this connection was closed and throws appropriate exception.
     * This method should be used for synchronous connection state check.
     *
     * @param reason Close reason.
     * @throws GridConnectionIdleClosedException If connection was closed as idle.
     * @throws GridClientClosedException If client was closed by by external call.
     * @throws GridClientConnectionResetException If connection was closed because of failure.
     */
    protected void checkClosed(GridClientConnectionCloseReason reason)
        throws GridConnectionIdleClosedException, GridClientConnectionResetException, GridClientClosedException {
        if (reason == GridClientConnectionCloseReason.CONN_IDLE)
            throw new GridConnectionIdleClosedException("Connection was closed by idle thread (will " +
                "reconnect): " + serverAddress());

        if (reason == GridClientConnectionCloseReason.FAILED)
            throw new GridClientConnectionResetException("Failed to perform request (connection failed before " +
                "message is sent): " + serverAddress());

        if (reason == GridClientConnectionCloseReason.CLIENT_CLOSED)
            throw new GridClientClosedException("Failed to perform request (connection was closed before " +
                "message is sent): " + serverAddress());
    }

    /**
     * Build appropriate exception from the given close reason.
     * This method should be used as a factory for exception to finish futures asynchronously.
     *
     * @param reason Close reason.
     * @param cause Cause of connection close, or {@code null} in case of regular close.
     * @return Exception.
     */
    protected GridClientException getCloseReasonAsException(GridClientConnectionCloseReason reason,
        @Nullable Throwable cause) {
        if (reason == GridClientConnectionCloseReason.CONN_IDLE)
            return new GridConnectionIdleClosedException("Connection was closed by idle thread: " + serverAddress());

        if (reason == GridClientConnectionCloseReason.FAILED)
            return new GridClientConnectionResetException("Failed to perform request (connection failed): " +
                serverAddress(), cause);

        if (reason == GridClientConnectionCloseReason.CLIENT_CLOSED)
            return new GridClientClosedException("Failed to perform request (client was closed): " + serverAddress());

        return null;
    }

    /**
     * @param reason Close reason.
     * @param cause Cause of connection close, or {@code null} in case of regular close.
     * @return Description of close reason for logging purpose.
     */
    protected String getCloseReasonMessage(GridClientConnectionCloseReason reason, @Nullable Throwable cause) {
        if (reason == GridClientConnectionCloseReason.CONN_IDLE)
            return "Connection was closed by idle thread";

        if (reason == GridClientConnectionCloseReason.FAILED)
            return cause != null ? "Connection failed, cause: " + cause.getMessage() : "Connection failed";

        if (reason == GridClientConnectionCloseReason.CLIENT_CLOSED)
            return "Client was closed";

        return null;
    }
}
