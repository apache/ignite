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
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cache statistics mode change discovery message.
 */
public class CacheStatisticsModeChangeMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Initial message flag mask. */
    private static final byte INITIAL_MESSAGE_MASK = 0x01;

    /** Statistics enabled flag mask. */
    private static final byte ENABLED_MASK = 0x02;

    /** Success flag mask. */
    private static final byte SUCCESS_MASK = 0x04;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** Request id. */
    private final UUID reqId;

    /** Cache names. */
    private final Collection<String> caches;

    /** Flags. */
    private byte flags;

    /**
     * Constructor for response.
     *
     * @param req Request message.
     */
    private CacheStatisticsModeChangeMessage(CacheStatisticsModeChangeMessage req) {
        this.flags = 0;
        this.reqId = req.reqId;
        this.caches = null;

        if (req.enabled())
            this.flags |= ENABLED_MASK;

        if (req.success())
            this.flags |= SUCCESS_MASK;
    }

    /**
     * Constructor for request.
     *
     * @param caches Collection of cache names.
     */
    public CacheStatisticsModeChangeMessage(UUID reqId, Collection<String> caches, boolean enabled) {
        this.flags = INITIAL_MESSAGE_MASK;
        this.reqId = reqId;
        this.caches = Collections.unmodifiableCollection(caches);

        if (enabled)
            this.flags |= ENABLED_MASK;

        this.flags |= SUCCESS_MASK;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return initial() ? new CacheStatisticsModeChangeMessage(this) : null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return initial();
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return Cache names.
     */
    public Collection<String> caches() {
        return Collections.unmodifiableCollection(caches);
    }

    /**
     * @return Statistic enabled.
     */
    public boolean enabled() {
        return (flags & ENABLED_MASK) != 0;
    }

    /**
     * Gets success flag.
     */
    public boolean success() {
        return (flags & SUCCESS_MASK) != 0;
    }

    /**
     * Sets success flag.
     */
    public void success(boolean success) {
        if (success)
            flags |= SUCCESS_MASK;
        else
            flags &= ~SUCCESS_MASK;
    }

    /**
     * @return Request id.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * Initial message flag.
     */
    public boolean initial() {
        return (flags & INITIAL_MESSAGE_MASK) != 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheStatisticsModeChangeMessage.class, this);
    }
}
