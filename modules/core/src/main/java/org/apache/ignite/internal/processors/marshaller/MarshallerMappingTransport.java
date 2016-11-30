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
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Provides capabilities of sending custom discovery events to propose new mapping or request missing mapping to {@link MarshallerContextImpl}.
 *
 * For more information about particular events see documentation of {@link GridMarshallerMappingProcessor}.
 */
public final class MarshallerMappingTransport {
    private final GridDiscoveryManager discoMgr;

    private final ConcurrentMap<MarshallerMappingItem, GridFutureAdapter<MappingExchangeResult>> mappingExchSyncMap;

    public MarshallerMappingTransport(GridDiscoveryManager discoMgr, ConcurrentMap<MarshallerMappingItem, GridFutureAdapter<MappingExchangeResult>> mappingExchSyncMap) {
        this.discoMgr = discoMgr;
        this.mappingExchSyncMap = mappingExchSyncMap;
    }

    public GridFutureAdapter<MappingExchangeResult> awaitMappingAcceptance(MarshallerMappingItem item, ConcurrentMap<Integer, MappedName> cache) {
        GridFutureAdapter<MappingExchangeResult> fut = new GridFutureAdapter<>();

        GridFutureAdapter<MappingExchangeResult> oldFut = mappingExchSyncMap.putIfAbsent(item, fut);
        if (oldFut != null)
            return oldFut;

        MappedName mappedName = cache.get(item.getTypeId());

        assert mappedName != null;

        //double check whether mapping is accepted, first check was in MarshallerContextImpl::registerClassName
        if (mappedName.isAccepted()) {
            fut.onDone(new MappingExchangeResult(false, null));
            mappingExchSyncMap.remove(item, fut);
        }

        return fut;
    }

    public GridFutureAdapter<MappingExchangeResult> proposeMapping(MarshallerMappingItem item, ConcurrentMap<Integer, MappedName> cache) throws IgniteCheckedException {
        boolean sendMsg = true;

        GridFutureAdapter<MappingExchangeResult> fut = new GridFutureAdapter<>();
        GridFutureAdapter<MappingExchangeResult> oldFut = mappingExchSyncMap.putIfAbsent(item, fut);
        if (oldFut == null) {
            //double check, first check was in caller: MarshallerContextImpl::registerClassName
            MappedName mapping = cache.get(item.getTypeId());

            if (mapping != null && mapping.isAccepted()) {
                MappingExchangeResult res;
                if (mapping.className().equals(item.getClsName()))
                    res = new MappingExchangeResult(false, null);
                else
                    res = new MappingExchangeResult(true, mapping.className());
                fut.onDone(res);
                mappingExchSyncMap.remove(item, fut);

                sendMsg = false;
            }
        }

        if (sendMsg) {
            MappingProposedMessage msg = new MappingProposedMessage(item, discoMgr.localNode().id());
            discoMgr.sendCustomEvent(msg);
        }

        return oldFut != null ? oldFut : fut;
    }

    public MappedName requestMapping(byte platformId, int typeId) throws IgniteCheckedException {
        discoMgr.sendCustomEvent(
                new MissingMappingRequestMessage(
                        new MarshallerMappingItem(platformId, typeId, null),
                        discoMgr.localNode().id()));

        return new MappedNameRequest();
    }
}
