/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.marshaller;

import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 * Provides capabilities of sending custom discovery events to propose new mapping
 * or request missing mapping to {@link MarshallerContextImpl}.
 *
 * For more information about particular events see documentation of {@link GridMarshallerMappingProcessor}.
 */
public final class MarshallerMappingTransport {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final GridDiscoveryManager discoMgr;

    /** */
    private final ConcurrentMap<MarshallerMappingItem, GridFutureAdapter<MappingExchangeResult>> mappingExchSyncMap;

    /** */
    private final ConcurrentMap<MarshallerMappingItem, ClientRequestFuture> clientReqSyncMap;

    /** */
    private volatile boolean stopping;

    /**
     * @param ctx Context.
     * @param mappingExchSyncMap Mapping exch sync map.
     * @param clientReqSyncMap Client request sync map.
     */
    MarshallerMappingTransport(
            GridKernalContext ctx,
            ConcurrentMap<MarshallerMappingItem, GridFutureAdapter<MappingExchangeResult>> mappingExchSyncMap,
            ConcurrentMap<MarshallerMappingItem, ClientRequestFuture> clientReqSyncMap
    ) {
        this.ctx = ctx;
        discoMgr = ctx.discovery();
        this.mappingExchSyncMap = mappingExchSyncMap;
        this.clientReqSyncMap = clientReqSyncMap;

        stopping = false;
    }

    /**
     * @param item Item.
     * @param cache Cache.
     */
    public GridFutureAdapter<MappingExchangeResult> awaitMappingAcceptance(
            MarshallerMappingItem item, ConcurrentMap<Integer,
            MappedName> cache
    ) {
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
    public GridFutureAdapter<MappingExchangeResult> requestMapping(
            MarshallerMappingItem item,
            ConcurrentMap<Integer, MappedName> cache
    ) {
        ClientRequestFuture newFut = new ClientRequestFuture(ctx, item, clientReqSyncMap);

        ClientRequestFuture oldFut = clientReqSyncMap.putIfAbsent(item, newFut);

        if (oldFut != null)
            return oldFut;

        MappedName mappedName = cache.get(item.typeId());

        if (mappedName != null) {
            newFut.onDone(MappingExchangeResult.createSuccessfulResult(mappedName.className()));

            return newFut;
        }

        newFut.requestMapping();

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
