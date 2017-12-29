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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

/**
 * Write-ahead log state manager. Manages WAL enable and disable.
 */
public class WalStateManager extends GridCacheSharedManagerAdapter {
    /** Client futures. */
    private final Map<UUID, GridFutureAdapter<Boolean>> userFuts = new HashMap<>();

    /** Operation mutex. */
    private final Object mux = new Object();

    /** Disconnected flag. */
    private boolean cliDisconnected;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        // TODO: Register IO listener for acks?
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        // TODO: Node stop.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        // TODO: Handle disconnect.
    }

    /** {@inheritDoc} */
    @Override public void onReconnected(boolean active) {
        // TODO: Handle reconnect.
    }

    /**
     * Initiate WAL mode change operation.
     *
     * @param cacheNames Cache names.
     * @param enabled Enabled flag.
     * @return Future completed when operation finished.
     */
    public IgniteInternalFuture<Boolean> initiate(Collection<String> cacheNames, boolean enabled) {
        synchronized (mux) {
            if (F.isEmpty(cacheNames))
                return errorFuture("Cache names cannot be empty.");

            if (cliDisconnected)
                return errorFuture("Failed to initiate WAL mode change because client node is disconnected.");

            // Prepare cache and group infos.
            Map<String, IgniteUuid> caches = new HashMap<>(cacheNames.size());

            CacheGroupDescriptor grpDesc = null;

            for (String cacheName : cacheNames) {
                DynamicCacheDescriptor cacheDesc = cacheProcessor().cacheDescriptor(cacheName);

                if (cacheDesc == null)
                    return errorFuture("Cache doesn't exits: " + cacheName);

                caches.put(cacheName, cacheDesc.deploymentId());

                CacheGroupDescriptor curGrpDesc = cacheDesc.groupDescriptor();

                if (grpDesc == null)
                    grpDesc = curGrpDesc;
                else if (!F.eq(grpDesc.deploymentId(), curGrpDesc.deploymentId())) {
                    return errorFuture("Cannot change WAL mode for caches from different cache groups [" +
                        "cache1=" + cacheNames.iterator().next() + ", grp1=" + grpDesc.groupName() +
                        ", cache2=" + cacheName + ", grp2=" + curGrpDesc.groupName() + ']');
                }
            }

            assert grpDesc != null;

            HashSet<String> grpCaches = new HashSet<>(grpDesc.caches().keySet());

            grpCaches.retainAll(cacheNames);

            if (!grpCaches.isEmpty()) {
                return errorFuture("Cannot change WAL mode because not all cache names belonging to the group are " +
                    "provided [group=" + grpDesc.groupName() + ", missingCaches=" + grpCaches + ']');
            }

            // Send request.
            final UUID opId = UUID.randomUUID();

            GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

            fut.listen(new IgniteInClosure<IgniteInternalFuture<Boolean>>() {
                @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                    synchronized (mux) {
                        userFuts.remove(opId);
                    }
                }
            });

            WalStateProposeMessage msg = new WalStateProposeMessage(opId, cctx.localNodeId(), caches,
                grpDesc.groupId(), enabled);

            userFuts.put(opId, fut);

            try {
                cctx.discovery().sendCustomEvent(msg);
            }
            catch (Exception e) {
                IgniteCheckedException e0 =
                    new IgniteCheckedException("Failed to initiate WAL mode change due to unexpected exception.", e);

                fut.onDone(e0);
            }

            return fut;
        }
    }

    /**
     * Handle propose message in discovery thread.
     *
     * @param msg Message.
     */
    public void onProposeDiscovery(WalStateProposeMessage msg) {
        synchronized (mux) {
            // Validate current caches state before deciding whether to process message further.
            if (!validatePropose(msg)) {
                msg.markIgnored();

                return;
            }

            if (hasWal()) {
                // TODO
            }
        }
    }

    /**
     * Validate propose message.
     *
     * @param msg Message.
     * @return {@code True} if message should be processed further, {@code false} if no further processing is needed.
     */
    private boolean validatePropose(WalStateProposeMessage msg) {
        GridFutureAdapter<Boolean> userFut = userFuts.get(msg.operationId());

        if (hasWal() || userFut != null) {
            // Is group still there?
            int grpId = msg.groupId();

            CacheGroupDescriptor grpDesc = cacheProcessor().cacheGroupDescriptors().get(msg.groupId());

            if (grpDesc == null) {
                completeWithError(userFut, "Failed to change WAL mode because some caches no longer exist: " +
                    msg.caches().keySet());

                return false;
            }

            // Are specified caches still there?
            for (Map.Entry<String, IgniteUuid> cache : msg.caches().entrySet()) {
                String cacheName = cache.getKey();

                DynamicCacheDescriptor cacheDesc = cacheProcessor().cacheDescriptor(cacheName);

                if (cacheDesc == null || !F.eq(cacheDesc.deploymentId(), cache.getValue())) {
                    completeWithError(userFut, "Cache doesn't exist: " + cacheName);

                    return false;
                }
            }

            // Are there any new caches in the group?
            HashSet<String> grpCacheNames = new HashSet<>(grpDesc.caches().keySet());

            grpCacheNames.retainAll(msg.caches().keySet());

            if (!grpCacheNames.isEmpty()) {
                completeWithError(userFut, "Cannot change WAL mode because not all cache names belonging to the " +
                    "group are provided [group=" + grpDesc.groupName() + ", missingCaches=" + grpCacheNames + ']');

                return false;
            }

            // If there are no pending WAL change requests and mode matches, then ignore and complete.
            if (!grpDesc.hasPendingWalChangeRequests() && grpDesc.walEnabled() == msg.enable()) {
                complete(userFut, false);

                return false;
            }

            // Everything is OK.
            return true;
        }
        else
            // Node without WAL, do not care.
            return false;
    }

    /**
     * Handle propose message which is synchronized with other cache state actions through exchange thread.
     *
     * @param msg Message.
     */
    public void onPropose(WalStateProposeMessage msg) {
        // TODO
    }

    /**
     * Handle finish message in discovery thread.
     *
     * @param msg Message.
     */
    public void onFinishDiscovery(WalStateFinishMessage msg) {
        // TODO
    }

    public void onAck(WalStateAckMessage msg) {
        // TODO
    }

    public void onNodeLeft(ClusterNode node) {
        // TODO
    }

    /**
     * Create future with error.
     *
     * @param errMsg Error message.
     * @return Future.
     */
    private static IgniteInternalFuture<Boolean> errorFuture(String errMsg) {
        return new GridFinishedFuture<Boolean>(new IgniteCheckedException(errMsg));
    }

    /**
     * Complete user future with normal result.
     *
     * @param userFut User future.
     * @param res Result.
     */
    private static void complete(@Nullable GridFutureAdapter<Boolean> userFut, boolean res) {
        if (userFut != null)
            userFut.onDone(res);
    }

    /**
     * Complete user future with error.
     *
     * @param errMsg Error message.
     */
    private static void completeWithError(@Nullable GridFutureAdapter<Boolean> userFut, String errMsg) {
        if (userFut != null)
            userFut.onDone(new IgniteCheckedException(errMsg));
    }

    /**
     * @return Cache processor.
     */
    private GridCacheProcessor cacheProcessor() {
        return cctx.cache();
    }

    /**
     * @return {@code True} if WAL exists.
     */
    private boolean hasWal() {
        return cctx.wal() != null;
    }
}
