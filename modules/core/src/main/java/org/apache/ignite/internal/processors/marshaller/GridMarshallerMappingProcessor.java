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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.MARSHALLER_PROC;
import static org.apache.ignite.internal.GridTopic.TOPIC_MAPPING_MARSH;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Processor responsible for managing custom {@link DiscoveryCustomMessage}
 * events for exchanging marshalling mappings between nodes in grid.
 *
 * In particular it processes two flows:
 * <ul>
 *     <li>
 *         Some node, server or client, wants to add new mapping for some class.
 *         In that case a pair of {@link MappingProposedMessage} and {@link MappingAcceptedMessage} events is used.
 *     </li>
 *     <li>
 *         As discovery events are delivered to clients asynchronously,
 *         client node may not have some mapping when server nodes in the grid are already allowed to use the mapping.
 *         In that situation client sends a {@link MissingMappingRequestMessage} request
 *         and processor handles it as well as {@link MissingMappingResponseMessage} message.
 *     </li>
 * </ul>
 */
public class GridMarshallerMappingProcessor extends GridProcessorAdapter {
    /** */
    private final MarshallerContextImpl marshallerCtx;

    /** */
    private final GridClosureProcessor closProc;

    /** */
    private final List<MappingUpdatedListener> mappingUpdatedLsnrs = new CopyOnWriteArrayList<>();

    /** */
    private final ConcurrentMap<MarshallerMappingItem, GridFutureAdapter<MappingExchangeResult>> mappingExchangeSyncMap
            = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<MarshallerMappingItem, ClientRequestFuture> clientReqSyncMap = new ConcurrentHashMap8<>();

    /**
     * @param ctx Kernal context.
     */
    public GridMarshallerMappingProcessor(GridKernalContext ctx) {
        super(ctx);

        marshallerCtx = ctx.marshallerContext();

        closProc = ctx.closure();
    }

    /** {@inheritDoc} */
    @Override public void start(boolean activeOnStart) throws IgniteCheckedException {
        GridDiscoveryManager discoMgr = ctx.discovery();
        GridIoManager ioMgr = ctx.io();

        MarshallerMappingTransport transport = new MarshallerMappingTransport(
                ctx,
                mappingExchangeSyncMap,
                clientReqSyncMap
        );

        marshallerCtx.onMarshallerProcessorStarted(ctx, transport);

        discoMgr.setCustomEventListener(MappingProposedMessage.class, new MarshallerMappingExchangeListener());

        discoMgr.setCustomEventListener(MappingAcceptedMessage.class, new MappingAcceptedListener());

        if (ctx.clientNode())
            ioMgr.addMessageListener(TOPIC_MAPPING_MARSH, new MissingMappingResponseListener());
        else
            ioMgr.addMessageListener(TOPIC_MAPPING_MARSH, new MissingMappingRequestListener(ioMgr));

        if (ctx.clientNode())
            ctx.event().addLocalEventListener(new GridLocalEventListener() {
                @Override public void onEvent(Event evt) {
                    DiscoveryEvent evt0 = (DiscoveryEvent) evt;

                    if (!ctx.isStopping()) {
                        for (ClientRequestFuture fut : clientReqSyncMap.values())
                            fut.onNodeLeft(evt0.eventNode().id());
                    }
                }
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /**
     * Adds a listener to be notified when mapping changes.
     *
     * @param lsnr listener for mapping updated events.
     */
    public void addMappingUpdatedListener(MappingUpdatedListener lsnr) {
        mappingUpdatedLsnrs.add(lsnr);
    }

    /**
     * Gets an iterator over all current mappings.
     *
     * @return Iterator over current mappings.
     */
    public Iterator<Map.Entry<Byte, Map<Integer, String>>> currentMappings() {
        return marshallerCtx.currentMappings();
    }

    /**
     *
     */
    private final class MissingMappingRequestListener implements GridMessageListener {
        /** */
        private final GridIoManager ioMgr;

        /**
         * @param ioMgr Io manager.
         */
        MissingMappingRequestListener(GridIoManager ioMgr) {
            this.ioMgr = ioMgr;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            assert msg instanceof MissingMappingRequestMessage : msg;

            MissingMappingRequestMessage msg0 = (MissingMappingRequestMessage) msg;

            byte platformId = msg0.platformId();
            int typeId = msg0.typeId();

            String resolvedClsName = marshallerCtx.resolveMissedMapping(platformId, typeId);

            try {
                ioMgr.sendToGridTopic(
                        nodeId,
                        TOPIC_MAPPING_MARSH,
                        new MissingMappingResponseMessage(platformId, typeId, resolvedClsName),
                        SYSTEM_POOL);
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send missing mapping response, node failed: " + nodeId);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send missing mapping response.", e);
            }
        }
    }

    /**
     *
     */
    private final class MissingMappingResponseListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            assert msg instanceof MissingMappingResponseMessage : msg;

            MissingMappingResponseMessage msg0 = (MissingMappingResponseMessage) msg;

            byte platformId = msg0.platformId();
            int typeId = msg0.typeId();
            String resolvedClsName = msg0.className();

            MarshallerMappingItem item = new MarshallerMappingItem(platformId, typeId, null);

            GridFutureAdapter<MappingExchangeResult> fut = clientReqSyncMap.get(item);

            if (fut != null) {
                if (resolvedClsName != null) {
                    marshallerCtx.onMissedMappingResolved(item, resolvedClsName);

                    fut.onDone(MappingExchangeResult.createSuccessfulResult(resolvedClsName));
                }
                else
                    fut.onDone(MappingExchangeResult.createFailureResult(
                            new IgniteCheckedException(
                                    "Failed to resolve mapping [platformId: "
                                            + platformId
                                            + ", typeId: "
                                            + typeId + "]")));
            }
        }
    }

    /**
     *
     */
    private final class MarshallerMappingExchangeListener implements CustomEventListener<MappingProposedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(
                AffinityTopologyVersion topVer,
                ClusterNode snd,
                MappingProposedMessage msg
        ) {
            if (!ctx.isStopping()) {
                if (msg.duplicated())
                    return;

                if (!msg.inConflict()) {
                    MarshallerMappingItem item = msg.mappingItem();
                    String conflictingName = marshallerCtx.onMappingProposed(item);

                    if (conflictingName != null) {
                        if (conflictingName.equals(item.className()))
                            msg.markDuplicated();
                        else
                            msg.conflictingWithClass(conflictingName);
                    }
                }
                else {
                    UUID origNodeId = msg.origNodeId();

                    if (origNodeId.equals(ctx.localNodeId())) {
                        GridFutureAdapter<MappingExchangeResult> fut = mappingExchangeSyncMap.get(msg.mappingItem());

                        assert fut != null: msg;

                        fut.onDone(MappingExchangeResult.createFailureResult(
                                duplicateMappingException(msg.mappingItem(), msg.conflictingClassName())));
                    }
                }
            }
        }

        /**
         * @param mappingItem Mapping item.
         * @param conflictingClsName Conflicting class name.
         */
        private IgniteCheckedException duplicateMappingException(
                MarshallerMappingItem mappingItem,
                String conflictingClsName
        ) {
            return new IgniteCheckedException("Duplicate ID [platformId="
                    + mappingItem.platformId()
                    + ", typeId="
                    + mappingItem.typeId()
                    + ", oldCls="
                    + conflictingClsName
                    + ", newCls="
                    + mappingItem.className() + "]");
        }
    }

    /**
     *
     */
    private final class MappingAcceptedListener implements CustomEventListener<MappingAcceptedMessage> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(
                AffinityTopologyVersion topVer,
                ClusterNode snd,
                MappingAcceptedMessage msg
        ) {
            final MarshallerMappingItem item = msg.getMappingItem();
            marshallerCtx.onMappingAccepted(item);

            closProc.runLocalSafe(new Runnable() {
                @Override public void run() {
                    for (MappingUpdatedListener lsnr : mappingUpdatedLsnrs)
                        lsnr.mappingUpdated(item.platformId(), item.typeId(), item.className());
                }
            });

            GridFutureAdapter<MappingExchangeResult> fut = mappingExchangeSyncMap.get(item);

            if (fut != null)
                fut.onDone(MappingExchangeResult.createSuccessfulResult(item.className()));
        }
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (!dataBag.commonDataCollectedFor(MARSHALLER_PROC.ordinal()))
            dataBag.addGridCommonData(MARSHALLER_PROC.ordinal(), marshallerCtx.getCachedMappings());
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(GridDiscoveryData data) {
        List<Map<Integer, MappedName>> mappings = (List<Map<Integer, MappedName>>) data.commonData();

        if (mappings != null) {
            for (int i = 0; i < mappings.size(); i++) {
                Map<Integer, MappedName> map;

                if ((map = mappings.get(i)) != null)
                    marshallerCtx.onMappingDataReceived((byte) i, map);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        cancelFutures(MappingExchangeResult.createFailureResult(new IgniteClientDisconnectedCheckedException(
                ctx.cluster().clientReconnectFuture(),
                "Failed to propose or request mapping, client node disconnected.")));
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        marshallerCtx.onMarshallerProcessorStop();

        cancelFutures(MappingExchangeResult.createExchangeDisabledResult());
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return MARSHALLER_PROC;
    }

    /**
     * @param res Response.
     */
    private void cancelFutures(MappingExchangeResult res) {
        for (GridFutureAdapter<MappingExchangeResult> fut : mappingExchangeSyncMap.values())
            fut.onDone(res);

        for (GridFutureAdapter<MappingExchangeResult> fut : clientReqSyncMap.values())
            fut.onDone(res);
    }
}
