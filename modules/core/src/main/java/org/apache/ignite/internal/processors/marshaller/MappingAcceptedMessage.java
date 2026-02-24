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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Is sent as an acknowledgement for successfully proposed new mapping (see {@link MappingProposedMessage}).
 *
 * If any nodes were waiting for this mapping to be accepted they will be unblocked on receiving this message.
 */
public class MappingAcceptedMessage implements DiscoveryCustomMessage, Message {
    /** */
    public static final short DIRECT_TYPE = 515;

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    IgniteUuid id;

    /** */
    @Order(1)
    byte platformId;

    /** */
    @Order(2)
    int typeId;

    /** */
    @Order(3)
    String clsName;

    /** */
    public MappingAcceptedMessage() {
        // No-op.
    }

    /**
     * @param item Item.
     */
    MappingAcceptedMessage(MarshallerMappingItem item) {
        this.id = IgniteUuid.randomUuid();
        this.platformId = item.platformId();
        this.typeId = item.typeId();
        this.clsName = item.className();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer, DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /** */
    MarshallerMappingItem getMappingItem() {
        return new MarshallerMappingItem(platformId, typeId, clsName);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return DIRECT_TYPE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MappingAcceptedMessage.class, this);
    }
}
