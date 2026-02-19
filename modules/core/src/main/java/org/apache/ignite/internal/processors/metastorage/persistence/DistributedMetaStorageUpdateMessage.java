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
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/** */
class DistributedMetaStorageUpdateMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Request ID. */
    @GridToStringInclude
    private final UUID reqId;

    /** */
    @GridToStringInclude
    private final String key;

    /** */
    private final byte[] valBytes;

    /** */
    private String errorMsg;

    /** */
    public DistributedMetaStorageUpdateMessage(UUID reqId, String key, byte[] valBytes) {
        this.reqId = reqId;
        this.key = key;
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** */
    public UUID requestId() {
        return reqId;
    }

    /** */
    public String key() {
        return key;
    }

    /** */
    public byte[] value() {
        return valBytes;
    }

    /** */
    public boolean isAckMessage() {
        return false;
    }

    /** */
    public void errorMessage(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    /** */
    protected String errorMessage() {
        return errorMsg;
    }

    /** {@inheritDoc} */
    @Override @Nullable public DiscoveryCustomMessage ackMessage() {
        return new DistributedMetaStorageUpdateAckMessage(reqId, errorMsg);
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
    @Override public String toString() {
        return S.toString(DistributedMetaStorageUpdateMessage.class, this);
    }
}
