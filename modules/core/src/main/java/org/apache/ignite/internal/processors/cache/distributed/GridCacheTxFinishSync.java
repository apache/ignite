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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Synchronization structure for asynchronous waiting for near tx finish responses based on per-node per-thread
 * basis.
 */
public class GridCacheTxFinishSync<K, V> {
    /** Cache context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Logger. */
    private IgniteLogger log;

    /** Nodes map. */
    private ConcurrentMap<Long, ThreadFinishSync> threadMap = new ConcurrentHashMap8<>();

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
     * @param threadId Thread ID started transaction.
     */
    public void onFinishSend(UUID nodeId, long threadId) {
        ThreadFinishSync threadSync = threadMap.get(threadId);

        if (threadSync == null)
            threadSync = F.addIfAbsent(threadMap, threadId, new ThreadFinishSync(threadId));

        threadSync.onSend(nodeId);
    }

    /**
     * @param nodeId Node ID to wait ack from.
     * @param threadId Thread ID to wait ack.
     * @return {@code null} if ack was received or future that will be completed when ack is received.
     */
    public IgniteInternalFuture<?> awaitAckAsync(UUID nodeId, long threadId) {
        ThreadFinishSync threadSync = threadMap.get(threadId);

        if (threadSync == null)
            return null;

        return threadSync.awaitAckAsync(nodeId);
    }

    /**
     * @param reconnectFut Reconnect future.
     */
    public void onDisconnected(IgniteFuture<?> reconnectFut) {
       for (ThreadFinishSync threadSync : threadMap.values())
            threadSync.onDisconnected(reconnectFut);

        threadMap.clear();
    }

    /**
     * Callback invoked when finish response is received from remote node.
     *
     * @param nodeId Node ID response was received from.
     * @param threadId Thread ID started transaction.
     */
    public void onAckReceived(UUID nodeId, long threadId) {
        ThreadFinishSync threadSync = threadMap.get(threadId);

        if (threadSync != null)
            threadSync.onReceive(nodeId);
    }

    /**
     * Callback invoked when node leaves grid.
     *
     * @param nodeId Left node ID.
     */
    public void onNodeLeft(UUID nodeId) {
        for (ThreadFinishSync threadSync : threadMap.values())
            threadSync.onNodeLeft(nodeId);
    }

    /**
     * Per-node sync.
     */
    private class ThreadFinishSync {
        /** Thread ID. */
        private long threadId;

        /** Thread map. */
        private final Map<UUID, TxFinishSync> nodeMap = new ConcurrentHashMap8<>();

        /**
         * @param threadId Thread ID.
         */
        private ThreadFinishSync(long threadId) {
            this.threadId = threadId;
        }

        /**
         * @param nodeId Node ID request being sent to.
         */
        public void onSend(UUID nodeId) {
            TxFinishSync sync = nodeMap.get(nodeId);

            if (sync == null) {
                sync = new TxFinishSync(nodeId, threadId);

                TxFinishSync old = nodeMap.put(nodeId, sync);

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
            TxFinishSync sync = nodeMap.get(nodeId);

            if (sync == null)
                return null;

            return sync.awaitAckAsync();
        }

        /**
         * @param reconnectFut Reconnect future.
         */
        public void onDisconnected(IgniteFuture<?> reconnectFut) {
            for (TxFinishSync sync : nodeMap.values())
                sync.onDisconnected(reconnectFut);

            nodeMap.clear();
        }

        /**
         * @param nodeId Node ID response received from.
         */
        public void onReceive(UUID nodeId) {
            TxFinishSync sync = nodeMap.get(nodeId);

            if (sync != null)
                sync.onReceive();
        }

        /**
         * @param nodeId Left node ID.
         */
        public void onNodeLeft(UUID nodeId) {
            TxFinishSync sync = nodeMap.remove(nodeId);

            if (sync != null)
                sync.onNodeLeft();
        }
    }

    /**
     * Tx sync. Allocated per-node per-thread.
     */
    private class TxFinishSync {
        /** Node ID. */
        private final UUID nodeId;

        /** Thread ID. */
        private final long threadId;

        /** Number of awaiting messages. */
        private int cnt;

        /** Node left flag. */
        private boolean nodeLeft;

        /** Pending await future. */
        private GridFutureAdapter<?> pendingFut;

        /**
         * @param nodeId Sync node ID. Used to construct correct error message.
         * @param threadId Thread ID.
         */
        private TxFinishSync(UUID nodeId, long threadId) {
            this.nodeId = nodeId;
            this.threadId = threadId;
        }

        /**
         * Callback invoked before sending finish request to remote nodes.
         * Will synchronously wait for previous finish response.
         */
        public void onSend() {
            synchronized (this) {
                if (log.isTraceEnabled())
                    log.trace("Moved transaction synchronizer to waiting state [nodeId=" + nodeId +
                        ", threadId=" + threadId + ']');

                assert cnt == 0 || nodeLeft;

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
                            ", threadId=" + threadId + ']');

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
                        ", threadId=" + threadId + ']');

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
    }
}
