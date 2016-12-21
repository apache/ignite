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

import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 * Provides capabilities of sending custom discovery events to propose new mapping or request missing mapping to {@link MarshallerContextImpl}.
 *
 * For more information about particular events see documentation of {@link GridMarshallerMappingProcessor}.
 */
public final class MarshallerMappingTransport {
    /** */
    private final GridDiscoveryManager discoMgr;

    /** */
    private final GridIoManager ioMgr;

    /** */
    private final ConcurrentMap<MarshallerMappingItem, GridFutureAdapter<MappingExchangeResult>> mappingExchSyncMap;

    /** */
    private volatile boolean stopping;

    /**
     * @param discoMgr Disco manager.
     * @param mappingExchSyncMap Mapping exch sync map.
     */
    MarshallerMappingTransport(GridDiscoveryManager discoMgr, GridIoManager ioMgr, ConcurrentMap<MarshallerMappingItem, GridFutureAdapter<MappingExchangeResult>> mappingExchSyncMap) {
        this.discoMgr = discoMgr;
        this.ioMgr = ioMgr;
        this.mappingExchSyncMap = mappingExchSyncMap;

        stopping = false;
    }

    /**
     * @param item Item.
     * @param cache Cache.
     */
    public GridFutureAdapter<MappingExchangeResult> awaitMappingAcceptance(MarshallerMappingItem item, ConcurrentMap<Integer, MappedName> cache) {
        GridFutureAdapter<MappingExchangeResult> fut = new MappingExchangeResultFuture(item);

        GridFutureAdapter<MappingExchangeResult> oldFut = mappingExchSyncMap.putIfAbsent(item, fut);
        if (oldFut != null)
            return oldFut;

        MappedName mappedName = cache.get(item.typeId());

        assert mappedName != null;

        //double check whether mapping is accepted, first check was in MarshallerContextImpl::registerClassName
        if (mappedName.accepted())
            fut.onDone(MappingExchangeResult.createSuccessfulResult(mappedName.className()));

        return fut;
    }

    /**
     * @param item Item.
     * @param cache Cache.
     */
    public GridFutureAdapter<MappingExchangeResult> proposeMapping(MarshallerMappingItem item, ConcurrentMap<Integer, MappedName> cache) throws IgniteCheckedException {
        GridFutureAdapter<MappingExchangeResult> fut = new MappingExchangeResultFuture(item);

        GridFutureAdapter<MappingExchangeResult> oldFut = mappingExchSyncMap.putIfAbsent(item, fut);

        if (oldFut != null)
            return oldFut;
        else {
            //double check, first check was in caller: MarshallerContextImpl::registerClassName
            MappedName mapping = cache.get(item.typeId());

            if (mapping != null) {
                String mappedClsName = mapping.className();

                if (!mappedClsName.equals(item.className()))
                    fut.onDone(MappingExchangeResult.createFailureResult(duplicateMappingException(item, mappedClsName)));
                else if (mapping.accepted())
                    fut.onDone(MappingExchangeResult.createSuccessfulResult(mappedClsName));
                else if (stopping)
                    fut.onDone(MappingExchangeResult.createExchangeDisabledResult());

                return fut;
            }
        }

        DiscoveryCustomMessage msg = new MappingProposedMessage(item, discoMgr.localNode().id());
        discoMgr.sendCustomEvent(msg);

        return fut;
    }

    /**
     * @param item Item.
     * @param cache Cache.
     */
    public GridFutureAdapter<MappingExchangeResult> requestMapping(MarshallerMappingItem item, ConcurrentMap<Integer, MappedName> cache) throws IgniteCheckedException {
        MappingExchangeResultFuture newFut = new MappingExchangeResultFuture(item);

        GridFutureAdapter<MappingExchangeResult> oldFut = mappingExchSyncMap.putIfAbsent(item, newFut);

        if (oldFut != null)
            return oldFut;

        if (oldFut == null) {
            MappedName mappedName = cache.get(item.typeId());
            if (mappedName != null) {
                newFut.onDone(MappingExchangeResult.createSuccessfulResult(mappedName.className()));

                return newFut;
            }
        }

        //if this node fails right before sending request, I need to repeat this with next oldestNode
        ClusterNode oldestNode = discoMgr.oldestAliveCacheServerNode(AffinityTopologyVersion.NONE);

        ioMgr.send(oldestNode, GridTopic.TOPIC_MAPPING_MARSH, new MissingMappingRequestMessage(item.platformId(), item.typeId()), GridIoPolicy.SYSTEM_POOL);

        return newFut;
    }

    /**
     * @param item Item.
     * @param mappedClsName Mapped class name.
     */
    private IgniteCheckedException duplicateMappingException(MarshallerMappingItem item, String mappedClsName) {
        return new IgniteCheckedException("Duplicate ID [platformId="
                + item.platformId()
                + ", typeId="
                + item.typeId()
                + ", oldCls="
                + mappedClsName
                + ", newCls="
                + item.className() + "]");
    }

    /** */
    public void markStopping() {
        stopping = true;
    }

    /** */
    public boolean stopping() {
        return stopping;
    }

    /**
     * Future to wait for mapping exchange result to arrive. Removes itself from map when completed.
     */
    private class MappingExchangeResultFuture extends GridFutureAdapter<MappingExchangeResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final MarshallerMappingItem mappingItem;

        /**
         * @param mappingItem Mapping item.
         */
        private MappingExchangeResultFuture(MarshallerMappingItem mappingItem) {
            this.mappingItem = mappingItem;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable MappingExchangeResult res, @Nullable Throwable err) {
            assert res != null;

            boolean done = super.onDone(res, null);

            if (done)
                mappingExchSyncMap.remove(mappingItem, this);

            return done;
        }
    }
}
