/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.future.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

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
    public IgniteFuture<?> awaitAckAsync(UUID nodeId, long threadId) {
        ThreadFinishSync threadSync = threadMap.get(threadId);

        if (threadSync == null)
            return null;

        return threadSync.awaitAckAsync(nodeId);
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
            }

            sync.onSend();
        }

        /**
         * Asynchronously awaits ack from node with given node ID.
         *
         * @param nodeId Node ID to wait ack from.
         * @return {@code null} if ack has been received or future that will be completed when ack is received.
         */
        public IgniteFuture<?> awaitAckAsync(UUID nodeId) {
            TxFinishSync sync = nodeMap.get(nodeId);

            if (sync == null)
                return null;

            return sync.awaitAckAsync();
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
        @Nullable public IgniteFuture<?> awaitAckAsync() {
            synchronized (this) {
                if (cnt == 0)
                    return null;

                if (nodeLeft)
                    return new GridFinishedFutureEx<>(new IgniteCheckedException("Failed to wait for finish synchronizer " +
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
    }
}
