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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.PartitionCountersNeighborcastRequest;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Represents partition update counters delivery to remote nodes.
 */
public class PartitionCountersNeighborcastFuture extends GridCacheCompoundIdentityFuture<Void> {
    /** */
    private final IgniteUuid futId = IgniteUuid.randomUuid();

    /** */
    @GridToStringExclude
    private boolean trackable = true;

    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    private final IgniteInternalTx tx;

    /** */
    private final IgniteLogger log;

    /** */
    public PartitionCountersNeighborcastFuture(
        IgniteInternalTx tx, GridCacheSharedContext<?, ?> cctx) {
        super(null);

        this.tx = tx;

        this.cctx = cctx;

        log = cctx.logger(CU.TX_MSG_RECOVERY_LOG_CATEGORY);
    }

    /**
     * Starts processing.
     */
    public void init() {
        if (log.isInfoEnabled()) {
            log.info("Starting delivery partition countres to remote nodes [txId=" + tx.nearXidVersion() +
                ", futId=" + futId);
        }

        HashSet<UUID> siblings = siblingBackups();

        cctx.mvcc().addFuture(this, futId);

        for (UUID peer : siblings) {
            List<PartitionUpdateCountersMessage> cntrs = cctx.tm().txHandler()
                .filterUpdateCountersForBackupNode(tx, cctx.node(peer));

            if (F.isEmpty(cntrs))
                continue;

            MiniFuture miniFut = new MiniFuture(peer);

            try {
                // we must add mini future before sending a message, otherwise mini future must miss completion
                add(miniFut);

                cctx.io().send(peer, new PartitionCountersNeighborcastRequest(cntrs, futId, tx.topologyVersion()), SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                if (!(e instanceof ClusterTopologyCheckedException))
                    log.warning("Failed to send partition counters to remote node [node=" + peer + ']', e);
                else
                    logNodeLeft(peer);

                miniFut.onDone();
            }
        }

        markInitialized();
    }

    /** */
    private HashSet<UUID> siblingBackups() {
        Map<UUID, Collection<UUID>> txNodes = tx.transactionNodes();

        assert txNodes != null;

        UUID locNodeId = cctx.localNodeId();

        HashSet<UUID> siblings = new HashSet<>();

        txNodes.values().stream()
            .filter(backups -> backups.contains(locNodeId))
            .forEach(siblings::addAll);

        siblings.remove(locNodeId);

        return siblings;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        boolean comp = super.onDone(res, err);

        if (comp)
            cctx.mvcc().removeFuture(futId);

        return comp;
    }

    /**
     * Processes a response from a remote peer. Completes a mini future for that peer.
     *
     * @param nodeId Remote peer node id.
     */
    public void onResult(UUID nodeId) {
        if (log.isInfoEnabled()) {
            log.info("Remote peer acked partition counters delivery [futId=" + futId +
                ", node=" + nodeId + ']');
        }

        completeMini(nodeId);
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        logNodeLeft(nodeId);

        // if a left node is one of remote peers then a mini future for it is completed successfully
        completeMini(nodeId);

        return true;
    }

    /** */
    private void completeMini(UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures()) {
            assert fut instanceof MiniFuture;

            MiniFuture mini = (MiniFuture)fut;

            if (mini.nodeId.equals(nodeId)) {
                cctx.kernalContext().closure().runLocalSafe(mini::onDone);

                return;
            }
        }

        if (log.isInfoEnabled()) {
            log.info("Failed to find mini future corresponding to node, can prevent parent future completion [" +
                "futId=" + futId +
                ", nodeId=" + nodeId + ']');
        }
    }

    /** */
    private void logNodeLeft(UUID nodeId) {
        if (log.isInfoEnabled()) {
            log.info("Failed during partition counters delivery to remote node. " +
                "Node left cluster (will ignore) [futId=" + futId +
                ", node=" + nodeId + ']');
        }
    }

    /** {@inheritDoc} */
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
    @Override public String toString() {
        return S.toString(PartitionCountersNeighborcastFuture.class, this, "innerFuts", futures());
    }

    /**
     * Component of compound parent future. Represents interaction with one of remote peers.
     */
    private static class MiniFuture extends GridFutureAdapter<Void> {
        /** */
        private final UUID nodeId;

        /** */
        private MiniFuture(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone());
        }
    }
}
