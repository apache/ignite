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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Synchronization structure for asynchronous waiting for near tx finish responses based on per-node per-tx basis.
 */
public class GridCacheTxFinishSync<K, V> {
    /** Cache context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Logger. */
    private IgniteLogger log;

    /** Tx map. */
    private ConcurrentMap<GridCacheVersion, TxFinishSync> xidMap = new ConcurrentHashMap<>();

    /**
     * @param cctx Cache context.
     */
    public GridCacheTxFinishSync(GridCacheSharedContext<K, V> cctx) {
        this.cctx = cctx;

        log = cctx.logger(GridCacheTxFinishSync.class);
    }

    /**
     * Callback invoked before finish request is sent to remote node.
     *
     * @param nodeId Node ID request being sent to.
     * @param xid Tx ID.
     */
    public void onFinishSend(UUID nodeId, GridCacheVersion xid) {
        TxFinishSync txSync = xidMap.computeIfAbsent(xid, sync -> new TxFinishSync(xid));

        synchronized (txSync) {
            txSync.onSend(nodeId);

            // Entries for this tx can be removed concurrently by other nodes events before we call onSend() method.
            xidMap.putIfAbsent(xid, txSync);
        }
    }

    /**
     * @param nodeId Node ID to wait ack from.
     * @param xid Tx ID.
     * @return {@code null} if ack was received or future that will be completed when ack is received.
     */
    public IgniteInternalFuture<?> awaitAckAsync(UUID nodeId, GridCacheVersion xid) {
        TxFinishSync txSync = xidMap.get(xid);

        if (txSync == null)
            return null;

        return txSync.awaitAckAsync(nodeId);
    }

    /**
     * @param reconnectFut Reconnect future.
     */
    public void onDisconnected(IgniteFuture<?> reconnectFut) {
       for (TxFinishSync txSync : xidMap.values())
            txSync.onDisconnected(reconnectFut);

        xidMap.clear();
    }

    /**
     * Callback invoked when finish response is received from remote node.
     *
     * @param nodeId Node ID response was received from.
     * @param xid Tx ID.
     */
    public void onAckReceived(UUID nodeId, GridCacheVersion xid) {
        TxFinishSync txSync = xidMap.get(xid);

        if (txSync != null) {
            txSync.onReceive(nodeId);

            synchronized (txSync) {
                if (txSync.isEmpty())
                    xidMap.remove(xid, txSync);
            }
        }
    }

    /**
     * Callback invoked when node leaves grid.
     *
     * @param nodeId Left node ID.
     */
    public void onNodeLeft(UUID nodeId) {
        for (TxFinishSync txSync : xidMap.values()) {
            txSync.onNodeLeft(nodeId);

            synchronized (txSync) {
                if (txSync.isEmpty())
                    xidMap.remove(txSync.xid, txSync);
            }
        }
    }

    /**
     * Callback invoked when failure occurs.
     *
     * @param nodeId Node ID.
     * @param xid Tx ID.
     * @param e Error.
     */
    public void onFail(UUID nodeId, GridCacheVersion xid, Exception e) {
        TxFinishSync txSync = xidMap.get(xid);

        if (txSync != null) {
            txSync.onFail(nodeId, e);

            synchronized (txSync) {
                if (txSync.isEmpty())
                    xidMap.remove(xid, txSync);
            }
        }
    }

    /**
     * Per-tx sync.
     */
    private class TxFinishSync {
        /** Tx ID. */
        private final GridCacheVersion xid;

        /** Thread map. */
        private final Map<UUID, TxNodeFinishSync> nodeMap = new ConcurrentHashMap<>();

        /**
         * @param xid Tx ID.
         */
        private TxFinishSync(GridCacheVersion xid) {
            this.xid = xid;
        }

        /**
         * @param nodeId Node ID request being sent to.
         */
        public void onSend(UUID nodeId) {
            TxNodeFinishSync sync = nodeMap.get(nodeId);

            if (sync == null) {
                sync = new TxNodeFinishSync(nodeId, xid);

                TxNodeFinishSync old = nodeMap.put(nodeId, sync);

                assert old == null : "Only user thread can add sync objects to the map.";

                // Recheck discovery only if added new sync.
                if (cctx.discovery().node(nodeId) == null) {
                    sync.onNodeLeft();

                    nodeMap.remove(nodeId);
                }
                else if (cctx.kernalContext().clientDisconnected()) {
                    sync.onDisconnected(cctx.kernalContext().cluster().clientReconnectFuture());

                    nodeMap.remove(nodeId);
                }
            }

            sync.onSend();
        }

        /**
         * Asynchronously awaits ack from node with given node ID.
         *
         * @param nodeId Node ID to wait ack from.
         * @return {@code null} if ack has been received or future that will be completed when ack is received.
         */
        public IgniteInternalFuture<?> awaitAckAsync(UUID nodeId) {
            TxNodeFinishSync sync = nodeMap.get(nodeId);

            if (sync == null)
                return null;

            return sync.awaitAckAsync();
        }

        /**
         * @param reconnectFut Reconnect future.
         */
        public void onDisconnected(IgniteFuture<?> reconnectFut) {
            for (TxNodeFinishSync sync : nodeMap.values())
                sync.onDisconnected(reconnectFut);

            nodeMap.clear();
        }

        /**
         * @param nodeId Node ID response received from.
         */
        public void onReceive(UUID nodeId) {
            TxNodeFinishSync sync = nodeMap.remove(nodeId);

            if (sync != null)
                sync.onReceive();
        }

        /**
         * @param nodeId Left node ID.
         */
        public void onNodeLeft(UUID nodeId) {
            TxNodeFinishSync sync = nodeMap.remove(nodeId);

            if (sync != null)
                sync.onNodeLeft();
        }

        /**
         * @param nodeId Node ID.
         * @param e Error.
         */
        public void onFail(UUID nodeId, Exception e) {
            TxNodeFinishSync sync = nodeMap.remove(nodeId);

            if (sync != null)
                sync.onFail(e);
        }

        /**
         * Ack's received from all alive nodes.
         */
        private boolean isEmpty() {
            return nodeMap.isEmpty();
        }
    }

    /**
     * Tx sync. Allocated per-node per-transaction.
     */
    private class TxNodeFinishSync {
        /** Node ID. */
        private final UUID nodeId;

        /** Tx ID. */
        private final GridCacheVersion xid;

        /** Number of awaiting messages. */
        private int cnt;

        /** Node left flag. */
        private boolean nodeLeft;

        /** Pending await future. */
        private GridFutureAdapter<?> pendingFut;

        /**
         * @param nodeId Sync node ID. Used to construct correct error message.
         * @param xid Tx ID.
         */
        private TxNodeFinishSync(UUID nodeId, GridCacheVersion xid) {
            this.nodeId = nodeId;
            this.xid = xid;
        }

        /**
         * Callback invoked before sending finish request to remote nodes.
         * Will synchronously wait for previous finish response.
         */
        public void onSend() {
            synchronized (this) {
                if (log.isTraceEnabled())
                    log.trace("Moved transaction synchronizer to waiting state [nodeId=" + nodeId +
                        ", xid=" + xid + ']');

                assert cnt == 0 || nodeLeft : cnt;

                if (nodeLeft)
                    return;

                // Do not create future for every send operation.
                cnt = 1;
            }
        }

        /**
         * Asynchronously waits for ack to be received from node.
         *
         * @return {@code null} if ack has been received, or future that will be completed when ack is received.
         */
        @Nullable public IgniteInternalFuture<?> awaitAckAsync() {
            synchronized (this) {
                if (cnt == 0)
                    return null;

                if (nodeLeft)
                    return new GridFinishedFuture<>(new IgniteCheckedException("Failed to wait for finish synchronizer " +
                        "state (node left grid): " + nodeId));

                if (pendingFut == null) {
                    if (log.isTraceEnabled())
                        log.trace("Creating transaction synchronizer future [nodeId=" + nodeId +
                            ", xid=" + xid + ']');

                    pendingFut = new GridFutureAdapter<>();
                }

                return pendingFut;
            }
        }

        /**
         * Callback for received response.
         */
        public void onReceive() {
            synchronized (this) {
                if (log.isTraceEnabled())
                    log.trace("Moving transaction synchronizer to completed state [nodeId=" + nodeId +
                        ", xid=" + xid + ']');

                cnt = 0;

                if (pendingFut != null) {
                    pendingFut.onDone();

                    pendingFut = null;
                }
            }
        }

        /**
         * Callback for node leave event.
         */
        public void onNodeLeft() {
            synchronized (this) {
                nodeLeft = true;

                if (pendingFut != null) {
                    pendingFut.onDone(new IgniteCheckedException("Failed to wait for transaction synchronizer " +
                        "completed state (node left grid): " + nodeId));

                    pendingFut = null;
                }
            }
        }

        /**
         * Client disconnected callback.
         *
         * @param reconnectFut Reconnect future.
         */
        public void onDisconnected(IgniteFuture<?> reconnectFut) {
            synchronized (this) {
                nodeLeft = true;

                if (pendingFut != null) {
                    IgniteClientDisconnectedCheckedException err = new IgniteClientDisconnectedCheckedException(
                        reconnectFut,
                        "Failed to wait for transaction synchronizer, client node disconnected: " + nodeId);
                    pendingFut.onDone(err);

                    pendingFut = null;
                }
            }
        }

        /**
         * Callback for request send failure.
         */
        public void onFail(Exception e) {
            synchronized (this) {
                nodeLeft = true;

                if (pendingFut != null) {
                    pendingFut.onDone(new IgniteCheckedException("Failed to wait for transaction synchronizer " +
                        "completed state, failed to send request to node: " + nodeId, e));

                    pendingFut = null;
                }
            }
        }
    }
}
