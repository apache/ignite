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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public class DistributedMetaStorageUpdateMessage implements DiscoveryCustomMessage, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    private IgniteUuid id;

    /** Request ID. */
    @Order(value = 1, method = "requestId")
    @GridToStringInclude
    private UUID reqId;

    /** */
    @Order(2)
    @GridToStringInclude
    private String key;

    /** */
    @Order(value = 3, method = "value")
    private byte[] valBytes;

    /** Empty constructor for {@link DiscoveryMessageFactory}. */
    public DistributedMetaStorageUpdateMessage() {
        // No-op.
    }

    /** */
    public DistributedMetaStorageUpdateMessage(UUID reqId, String key, byte[] valBytes) {
        id = IgniteUuid.randomUuid();

        this.reqId = reqId;
        this.key = key;
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** */
    public void id(IgniteUuid id) {
        this.id = id;
    }

    /** */
    public UUID requestId() {
        return reqId;
    }

    /** */
    public void requestId(UUID reqId) {
        this.reqId = reqId;
    }

    /** */
    public String key() {
        return key;
    }

    /** */
    public void key(String key) {
        this.key = key;
    }

    /** */
    public byte[] value() {
        return valBytes;
    }

    /** */
    public void value(byte[] valBytes) {
        this.valBytes = valBytes;
    }

    /** */
    public boolean isAckMessage() {
        return false;
    }

    /** {@inheritDoc} */
    @Override @Nullable public DiscoveryCustomMessage ackMessage() {
        return new DistributedMetaStorageUpdateAckMessage(reqId);
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(
        GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer,
        DiscoCache discoCache
    ) {
        throw new UnsupportedOperationException("createDiscoCache");
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 20;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedMetaStorageUpdateMessage.class, this);
    }
}
