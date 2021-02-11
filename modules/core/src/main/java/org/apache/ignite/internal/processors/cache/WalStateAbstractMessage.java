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

package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * WAL state change abstract message.
 */
public abstract class WalStateAbstractMessage implements DiscoveryCustomMessage {
    /** Message ID */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Unique operation ID. */
    private final UUID opId;

    /** Group ID. */
    private int grpId;

    /** Group deployment ID. */
    private IgniteUuid grpDepId;

    /** Message that should be processed through exchange thread. */
    @GridToStringExclude
    private transient WalStateProposeMessage exchangeMsg;

    /**
     * Constructor.
     *
     * @param opId Unique operation ID.
     * @param grpId Group ID.
     * @param grpDepId Group deployment ID.
     */
    protected WalStateAbstractMessage(UUID opId, int grpId, IgniteUuid grpDepId) {
        this.opId = opId;
        this.grpId = grpId;
        this.grpDepId = grpDepId;
    }

    /**
     * @return Unique operation ID.
     */
    public UUID operationId() {
        return opId;
    }

    /**
     * @return Group ID.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @return Group deployment ID.
     */
    public IgniteUuid groupDeploymentId() {
        return grpDepId;
    }

    /**
     * @return {@code True} if exchange is needed.
     */
    public boolean needExchange() {
        return exchangeMsg != null;
    }

    /**
     * Get exchange message.
     *
     * @return Massage or {@code null} if no processing is required.
     */
    @Nullable public WalStateProposeMessage exchangeMessage() {
        return exchangeMsg;
    }

    /**
     * Set message that will be processed through exchange thread later on.
     *
     * @param exchangeMsg Message.
     */
    public void exchangeMessage(WalStateProposeMessage exchangeMsg) {
        this.exchangeMsg = exchangeMsg;
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
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return discoCache.copy(topVer, null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalStateAbstractMessage.class, this);
    }
}
