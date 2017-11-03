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

package org.apache.ignite.internal.processors.marshaller;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Future responsible for requesting missing marshaller mapping from one of available server nodes.
 *
 * Handles scenarios when server nodes leave cluster. If node that was requested for mapping leaves the cluster or fails,
 * mapping is automatically requested from the next node available in topology.
 */
final class ClientRequestFuture extends GridFutureAdapter<MappingExchangeResult> {
    /** */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** */
    private static IgniteLogger log;

    /** */
    private final GridIoManager ioMgr;

    /** */
    private final GridDiscoveryManager discoMgr;

    /** */
    private final MarshallerMappingItem item;

    /** */
    private final Map<MarshallerMappingItem, ClientRequestFuture> syncMap;

    /** */
    private final Queue<ClusterNode> aliveSrvNodes;

    /** */
    private ClusterNode pendingNode;

    /**
     * @param ctx Context.
     * @param item Item.
     * @param syncMap Sync map.
     */
    ClientRequestFuture(
            GridKernalContext ctx,
            MarshallerMappingItem item,
            Map<MarshallerMappingItem, ClientRequestFuture> syncMap
    ) {
        ioMgr = ctx.io();
        discoMgr = ctx.discovery();
        aliveSrvNodes = new LinkedList<>(discoMgr.aliveServerNodes());
        this.item = item;
        this.syncMap = syncMap;

        if (log == null)
            log = U.logger(ctx, logRef, ClientRequestFuture.class);
    }

    /**
     *
     */
    void requestMapping() {
        boolean noSrvsInCluster;

        synchronized (this) {
            while (!aliveSrvNodes.isEmpty()) {
                ClusterNode srvNode = aliveSrvNodes.poll();

                try {
                    ioMgr.sendToGridTopic(
                            srvNode,
                            GridTopic.TOPIC_MAPPING_MARSH,
                            new MissingMappingRequestMessage(
                                    item.platformId(),
                                    item.typeId()),
                            GridIoPolicy.SYSTEM_POOL);

                    if (discoMgr.node(srvNode.id()) == null)
                        continue;

                    pendingNode = srvNode;

                    break;
                }
                catch (IgniteCheckedException ignored) {
                    U.warn(log,
                            "Failed to request marshaller mapping from remote node (proceeding with the next one): "
                                    + srvNode);
                }
            }

            noSrvsInCluster = pendingNode == null;
        }

        if (noSrvsInCluster)
            onDone(MappingExchangeResult.createFailureResult(
                    new IgniteCheckedException(
                            "All server nodes have left grid, cannot request mapping [platformId: "
                                    + item.platformId()
                                    + "; typeId: "
                                    + item.typeId() + "]")));
    }

    /**
     * @param nodeId Node ID.
     * @param res Mapping Request Result.
     */
    void onResponse(UUID nodeId, MappingExchangeResult res) {
        MappingExchangeResult res0 = null;

        synchronized (this) {
            if (pendingNode != null && pendingNode.id().equals(nodeId))
                res0 = res;
        }

        if (res0 != null)
            onDone(res0);
    }

    /**
     * If left node is actually the one latest mapping request was sent to,
     * request is sent again to the next node in topology.
     *
     * @param leftNodeId Left node id.
     */
    void onNodeLeft(UUID leftNodeId) {
        boolean reqAgain = false;

        synchronized (this) {
            if (pendingNode != null && pendingNode.id().equals(leftNodeId)) {
                aliveSrvNodes.remove(pendingNode);

                pendingNode = null;

                reqAgain = true;
            }
        }

        if (reqAgain)
            requestMapping();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable MappingExchangeResult res, @Nullable Throwable err) {
        assert res != null;

        boolean done = super.onDone(res, err);

        if (done)
            syncMap.remove(item);

        return done;
    }
}
