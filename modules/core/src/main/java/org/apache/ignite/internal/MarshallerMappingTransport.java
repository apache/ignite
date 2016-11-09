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
package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.marshaller.MappingProposedMessageBuilder;
import org.apache.ignite.internal.processors.marshaller.MappingRequestFuture;
import org.apache.ignite.internal.processors.marshaller.MissingMappingRequestMessageBuilder;

/**
 * Provides capabilities of sending custom discovery events to propose new mapping or request missing mapping to {@link MarshallerContextImpl}.
 *
 * For more information about particular events see documentation of {@link org.apache.ignite.internal.processors.marshaller.GridMarshallerMappingProcessor}.
 */
final class MarshallerMappingTransport {
    private final GridDiscoveryManager discoMgr;

    MarshallerMappingTransport(GridDiscoveryManager discoMgr) {
        this.discoMgr = discoMgr;
    }

    void proposeMapping(byte platformId, int typeId, String clsName) throws IgniteCheckedException {
        discoMgr.sendCustomEvent(new MappingProposedMessageBuilder().forType(typeId).addPlatformMapping(platformId, clsName).build());
    }

    MappedName requestMapping(byte platformId, int typeId) throws IgniteCheckedException {
        discoMgr.sendCustomEvent(new MissingMappingRequestMessageBuilder().fromNode(discoMgr.localNode().id()).forType(typeId).addPlatform(platformId).build());

        return new MappingRequestFuture();
    }
}
