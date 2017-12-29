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
import org.apache.ignite.lang.IgniteUuid;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Write-ahead log state processor. Manages WAL enable and disable.
 */
public class WalStateProcessor extends GridCacheSharedManagerAdapter {
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

            // Send request.
            UUID opId = UUID.randomUUID();

            GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

            WalStateProposeMessage msg = new WalStateProposeMessage(opId, caches, grpDesc.groupId(), enabled);

            userFuts.put(opId, fut);

            try {
                cctx.discovery().sendCustomEvent(msg);
            }
            catch (Exception e) {
                IgniteCheckedException e0 =
                    new IgniteCheckedException("Failed to initiate WAL mode change due to unexpected exception.", e);

                fut.onDone(e0);

                userFuts.remove(opId);
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
        // TODO
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
     * @param msg Error message.
     * @return Future.
     */
    private static IgniteInternalFuture<Boolean> errorFuture(String msg) {
        return new GridFinishedFuture<Boolean>(new IgniteCheckedException(msg));
    }

    /**
     * @return Cache processor.
     */
    private GridCacheProcessor cacheProcessor() {
        return cctx.cache();
    }
}
