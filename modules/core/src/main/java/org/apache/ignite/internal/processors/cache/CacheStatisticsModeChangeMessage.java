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

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
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
 * Cache statistics mode change discovery message.
 */
public class CacheStatisticsModeChangeMessage implements DiscoveryCustomMessage, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Initial message flag mask. */
    private static final byte INITIAL_MSG_MASK = 0x01;

    /** Statistics enabled flag mask. */
    private static final byte ENABLED_MASK = 0x02;

    /** Custom message ID. */
    @Order(0)
    private IgniteUuid id;

    /** Request id. */
    @Order(value = 1, method = "requestId")
    private UUID reqId;

    /** Cache names. */
    @Order(2)
    private Collection<String> caches;

    /** Flags. */
    @Order(3)
    private byte flags;

    /**
     * Default constructor.
     */
    public CacheStatisticsModeChangeMessage() {
        // No-op.
    }

    /**
     * Constructor for response.
     *
     * @param req Request message.
     */
    private CacheStatisticsModeChangeMessage(IgniteUuid id, CacheStatisticsModeChangeMessage req) {
        this.id = id;
        reqId = req.reqId;
        caches = null;

        if (req.enabled())
            flags = ENABLED_MASK;
        else
            flags = 0;
    }

    /**
     * Constructor for request.
     *
     * @param caches Collection of cache names.
     */
    public CacheStatisticsModeChangeMessage(IgniteUuid id, UUID reqId, Collection<String> caches, boolean enabled) {
        this.id = id;
        this.reqId = reqId;
        this.caches = Collections.unmodifiableCollection(caches);

        byte flags = INITIAL_MSG_MASK;

        if (enabled)
            flags |= ENABLED_MASK;

        this.flags = flags;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return initial() ? new CacheStatisticsModeChangeMessage(IgniteUuid.randomUuid(), this) : null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     * @param id Custom message ID.
     */
    public void id(IgniteUuid id) {
        this.id = id;
    }

    /**
     * @return Request id.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * @param reqId Request id.
     */
    public void requestId(UUID reqId) {
        this.reqId = reqId;
    }

    /**
     * @return Cache names.
     */
    public Collection<String> caches() {
        return caches;
    }

    /**
     * @param caches Cache names.
     */
    public void caches(Collection<String> caches) {
        this.caches = caches;
    }

    /**
     * @return Flags.
     */
    public byte flags() {
        return flags;
    }

    /**
     * @param flags Flags.
     */
    public void flags(byte flags) {
        this.flags = flags;
    }

    /**
     * Initial message flag.
     */
    public boolean initial() {
        return (flags & INITIAL_MSG_MASK) != 0;
    }

    /**
     * @return Statistic enabled.
     */
    public boolean enabled() {
        return (flags & ENABLED_MASK) != 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheStatisticsModeChangeMessage.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 500;
    }
}
