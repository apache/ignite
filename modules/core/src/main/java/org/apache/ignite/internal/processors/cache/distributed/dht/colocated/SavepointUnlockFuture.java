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

package org.apache.ignite.internal.processors.cache.distributed.dht.colocated;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheLockTimeoutException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridRollbackToSavepointResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Used in process of unlocking keys during rollback to savepoint to wait responses from nodes.
 */
public final class SavepointUnlockFuture extends GridCacheCompoundIdentityFuture<Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger for io messages. */
    private static IgniteLogger msgLog;

    /** Logger. */
    private static IgniteLogger log;

    /** Future ID. */
    private final IgniteUuid futId;

    /** Lock version. */
    private final GridCacheVersion lockVer;

    /** Error. */
    private volatile Throwable err;

    /** Transaction. */
    @GridToStringExclude
    private final GridNearTxLocal tx;

    /** Trackable flag (here may be non-volatile). */
    private boolean trackable;

    /**
     * @param tx Transaction.
     */
    public SavepointUnlockFuture(GridNearTxLocal tx) {
        super(CU.boolReducer());

        this.tx = tx;
        lockVer = tx.xidVersion();
        futId = IgniteUuid.randomUuid();

        ignoreInterrupts();

        if (log == null) {
            msgLog = tx.context().txLockMessageLogger();
            log = U.logger(tx.context().kernalContext(), logRef, SavepointUnlockFuture.class);
        }
    }

    /**
     * @return Future ID.
     */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<?> fut : futures()) {
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onResult(newTopologyException(nodeId));

                    found = true;

                    break;
                }
            }
        }

        if (!found && msgLog.isDebugEnabled()) {
            msgLog.debug("Savepoint unlock future does not have mapping for left node (ignoring) [nodeId=" + nodeId +
                ", fut=" + this + ']');
        }

        return found;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridRollbackToSavepointResponse res) {
        if (!isDone()) {
            MiniFuture mini = miniFuture(res.miniId());

            if (mini != null) {
                assert mini.node().id().equals(nodeId);

                mini.onResult(res);

                if (isAllMiniFutsDone())
                    onDone(true, null);

                return;
            }

            U.warn(msgLog, "Collocated savepoint unlock fut, failed to find mini future [txId=" + lockVer +
                ", node=" + nodeId +
                ", res=" + res +
                ", fut=" + this + ']');
        }
        else if (msgLog.isDebugEnabled()) {
            msgLog.debug("Collocated savepoint unlock fut, response for finished future [txId=" + lockVer +
                ", node=" + nodeId + ']');
        }
    }

    /**
     * @return {@code True} if all mini futures are finished. Otherwise - {@code false}.
     */
    private boolean isAllMiniFutsDone() {
        for (IgniteInternalFuture<?> fut : futures()) {
            if (isMini(fut) && !fut.isDone())
                return false;
        }

        return true;
    }

    /**
     * Finds pending mini future by the given mini ID.
     *
     * @param miniId Mini ID to find.
     * @return Mini future.
     */
    @SuppressWarnings({"ForLoopReplaceableByForEach", "IfMayBeConditional"})
    private MiniFuture miniFuture(int miniId) {
        // We iterate directly over the futs collection here to avoid copy.
        synchronized (this) {
            int size = futuresCountNoLock();

            // Avoid iterator creation.
            for (int i = 0; i < size; i++) {
                IgniteInternalFuture<Boolean> fut = future(i);

                if (!isMini(fut))
                    continue;

                MiniFuture mini = (MiniFuture)fut;

                if (mini.futureId() == miniId) {
                    if (!mini.isDone())
                        return mini;
                    else
                        return null;
                }
            }
        }

        return null;
    }

    /**
     * @param t Error.
     */
    private synchronized void onError(Throwable t) {
        if (err == null)
            err = t;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        if (onCancelled())
            onComplete(false);

        return isCancelled();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Boolean success, Throwable err) {
        if (log.isDebugEnabled())
            log.debug("Received onDone(..) callback [success=" + success + ", err=" + err + ", fut=" + this + ']');

        if (isDone())
            return false;

        if (err != null) {
            onError(err);

            success = false;

            for (IgniteInternalFuture<?> fut : futures()) {
                if (isMini(fut))
                    ((MiniFuture)fut).onDone(false);
            }
        }

        return onComplete(success);
    }

    /**
     * Completeness callback.
     *
     * @param success {@code True} if unlock was successful.
     * @return {@code True} if complete by this operation.
     */
    private boolean onComplete(boolean success) {
        if (log.isDebugEnabled())
            log.debug("Received onComplete(..) callback [success=" + success + ", fut=" + this + ']');

        tx.context().tm().txContext(tx);

        if (super.onDone(success, err)) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return futId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @Override public String apply(IgniteInternalFuture<?> f) {
                if (isMini(f)) {
                    MiniFuture m = (MiniFuture)f;

                    return "[node=" + m.node().id() + ", loc=" + m.node().isLocal() + ", done=" + f.isDone() + "]";
                }
                else
                    return "[loc=true, done=" + f.isDone() + "]";
            }
        });

        return S.toString(SavepointUnlockFuture.class, this,
            "innerFuts", futs,
            "super", super.toString());
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<?> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     * Creates new topology exception for cases when primary node leaves grid during mapping.
     *
     * @param nodeId Node ID.
     * @return Topology exception with user-friendly message.
     */
    private ClusterTopologyCheckedException newTopologyException(UUID nodeId) {
        ClusterTopologyCheckedException topEx = new ClusterTopologyCheckedException(
            "Failed to unlock keys for savepoint (primary node left grid, retry transaction if possible) [node=" +
                nodeId + ']');

        topEx.retryReadyFuture(tx.context().nextAffinityReadyFuture(tx.topologyVersion()));

        return topEx;
    }

    /**
     * @param node Primary for keys.
     * @param keys Key count.
     * @return New MiniFuture id.
     */
    public Integer newMiniFutureId(ClusterNode node, Map<Integer, List<KeyCacheObject>> keys) {
        synchronized (this) {
            MiniFuture fut = new MiniFuture(node, futuresCountNoLock(), keys);

            add(fut);

            return fut.futId;
        }
    }

    /**
     * Mini-futures are only waiting on a single node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<Boolean> {
        /** */
        private final int futId;

        /** Node. */
        private final ClusterNode node;

        /** Keys. */
        @GridToStringInclude
        private final Map<Integer, List<KeyCacheObject>> keys;

        /** */
        private boolean rcvRes;

        /**
         * @param node Node.
         * @param futId Mini future ID.
         */
        private MiniFuture(ClusterNode node, int futId, Map<Integer, List<KeyCacheObject>> keys) {
            this.node = node;
            this.futId = futId;
            this.keys = keys;
        }

        /**
         * @return Future ID.
         */
        int futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * @return Keys.
         */
        public Map<Integer, List<KeyCacheObject>> keys() {
            return keys;
        }

        /**
         * @param e Node left exception.
         */
        void onResult(ClusterTopologyCheckedException e) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Collocated lock fut, mini future node left [txId=" + lockVer +
                    ", nodeId=" + node.id() + ']');
            }

            if (isDone())
                return;

            synchronized (this) {
                if (rcvRes)
                    return;

                rcvRes = true;
            }

            if (tx != null)
                tx.removeMapping(node.id());

            SavepointUnlockFuture.this.onDone(false, e);

            onDone(true);
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridRollbackToSavepointResponse res) {
            synchronized (this) {
                if (rcvRes)
                    return;

                rcvRes = true;
            }

            if (res.error() != null) {
                if (log.isDebugEnabled())
                    log.debug("Finishing mini future with an error due to error in response [miniFut=" + this +
                        ", res=" + res + ']');

                // Fail.
                if (res.error() instanceof GridCacheLockTimeoutException)
                    onDone(false);
                else
                    onDone(res.error());

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Processed response for entry [res=" + res + ", keys=" + keys + ']');

            onDone(true);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "node", node.id(), "super", super.toString());
        }
    }
}
