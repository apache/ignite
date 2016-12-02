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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataContainer;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataContainer.GridDiscoveryData;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.MARSHALLER_PROC;

/**
 * Processor responsible for managing custom {@link org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage} events for exchanging marshalling mappings between nodes in grid.
 *
 * In particular it processes two flows:
 * <ul>
 *     <li>
 *         Some node, server or client, wants to add new mapping for some class.
 *         In that case a pair of {@link MappingProposedMessage} and {@link MappingAcceptedMessage} events is used.
 *     </li>
 *     <li>
 *         As discovery events are delivered to clients asynchronously, client node may not have some mapping when server nodes in the grid are already allowed to use the mapping.
 *         In that situation client sends a {@link MissingMappingRequestMessage} request and processor handles it as well as {@link MissingMappingResponseMessage} message.
 *     </li>
 * </ul>
 */
public class GridMarshallerMappingProcessor extends GridProcessorAdapter {
    /** */
    private final MarshallerContextImpl marshallerCtx;

    /** */
    private ConcurrentMap<MarshallerMappingItem, GridFutureAdapter<MappingExchangeResult>> mappingExchangeSyncMap = new ConcurrentHashMap8<>();

    /**
     * @param ctx Kernal context.
     */
    public GridMarshallerMappingProcessor(GridKernalContext ctx) {
        super(ctx);

        marshallerCtx = ctx.marshallerContext();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        GridDiscoveryManager discoMgr = ctx.discovery();
        MarshallerMappingTransport transport = new MarshallerMappingTransport(discoMgr, mappingExchangeSyncMap);
        marshallerCtx.onMarshallerProcessorStarted(ctx, transport);

        discoMgr.setCustomEventListener(MappingProposedMessage.class, new MarshallerMappingExchangeListener());

        discoMgr.setCustomEventListener(MappingAcceptedMessage.class, new MappingAcceptedListener());

        discoMgr.setCustomEventListener(MappingRejectedMessage.class, new MappingRejectedListener());

        discoMgr.setCustomEventListener(MissingMappingRequestMessage.class, new MissingMappingRequestListener());

        discoMgr.setCustomEventListener(MissingMappingResponseMessage.class, new MissingMappingResponseListener());
    }

    /**
     *
     */
    private final class MarshallerMappingExchangeListener implements CustomEventListener<MappingProposedMessage> {

        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, MappingProposedMessage msg) {
            if (!ctx.isStopping())
                if (!msg.inConflict() && !msg.duplicated()) {
                    MarshallerMappingItem item = msg.mappingItem();
                    String conflictingName = marshallerCtx.onMappingProposed(item);

                    if (conflictingName != null)
                        if (conflictingName.equals(item.className()))
                            msg.markDuplicated();
                        else {
                            msg.markInConflict();
                            msg.conflictingClassName(conflictingName);
                        }
                }
        }
    }

    /**
     *
     */
    private final class MappingAcceptedListener implements CustomEventListener<MappingAcceptedMessage> {

        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, MappingAcceptedMessage msg) {
            MarshallerMappingItem item = msg.getMappingItem();
            marshallerCtx.onMappingAccepted(item);

            GridFutureAdapter<MappingExchangeResult> fut = mappingExchangeSyncMap.get(item);

            if (fut != null) {
                fut.onDone(new MappingExchangeResult(false, null));
                mappingExchangeSyncMap.remove(item, fut);
            }
        }
    }

    /**
     *
     */
    private final class MappingRejectedListener implements CustomEventListener<MappingRejectedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, MappingRejectedMessage msg) {
            UUID origNodeId = msg.origNodeId();
            UUID locNodeId = ctx.localNodeId();

            if (locNodeId.equals(origNodeId)) {
                GridFutureAdapter<MappingExchangeResult> fut = mappingExchangeSyncMap.get(msg.getOrigMappingItem());

                if (fut != null)
                    fut.onDone(new MappingExchangeResult(true, msg.getConflictingClsName()));
            }
        }
    }

    /**
     *
     */
    private final class MissingMappingRequestListener implements CustomEventListener<MissingMappingRequestMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, MissingMappingRequestMessage msg) {
            if (!ctx.clientNode()
                    && !msg.resolved())
                msg.resolvedClsName(
                        marshallerCtx.resolveMissedMapping(msg.mappingItem()));
        }
    }

    /**
     *
     */
    private final class MissingMappingResponseListener implements CustomEventListener<MissingMappingResponseMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, MissingMappingResponseMessage msg) {
            UUID locNodeId = ctx.localNodeId();
            if (ctx.clientNode() && locNodeId.equals(msg.origNodeId())) {
                marshallerCtx.onMissedMappingResolved(msg.marshallerMappingItem(), msg.resolvedClassName());

                GridFutureAdapter<MappingExchangeResult> fut = mappingExchangeSyncMap.get(msg.marshallerMappingItem());
                if (fut != null) {
                    String resolvedClsName = msg.resolvedClassName();
                    boolean resolutionFailed = resolvedClsName == null;
                    fut.onDone(new MappingExchangeResult(resolutionFailed, resolvedClsName));
                }

            }
        }
    }

    /** {@inheritDoc} */
    @Override public void collectDiscoveryData(DiscoveryDataContainer dataContainer) {
        if (!ctx.localNodeId().equals(dataContainer.getJoiningNodeId()))
            if (!dataContainer.isCommonDataCollectedFor(MARSHALLER_PROC.ordinal()))
                dataContainer.addGridCommonData(MARSHALLER_PROC.ordinal(), marshallerCtx.getCachedMappings());
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(GridDiscoveryData data) {
        Map<Byte, ConcurrentMap<Integer, MappedName>> marshallerMappings = (Map<Byte, ConcurrentMap<Integer, MappedName>>) data.commonData();

        if (marshallerMappings != null)
            for (Map.Entry<Byte, ConcurrentMap<Integer, MappedName>> e : marshallerMappings.entrySet())
                marshallerCtx.applyPlatformMapping(e.getKey(), e.getValue());
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return MARSHALLER_PROC;
    }
}
