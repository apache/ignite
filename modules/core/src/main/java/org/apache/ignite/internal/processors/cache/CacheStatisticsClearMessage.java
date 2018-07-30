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
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cache statistics clear discovery message.
 */
public class CacheStatisticsClearMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Initial message flag mask. */
    private static final byte INITIAL_MSG_MASK = 0x01;

    /** Custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Request id. */
    private final UUID reqId;

    /** Cache names. */
    private final Collection<String> caches;

    /** Flags. */
    private final byte flags;

    /**
     * Constructor for request.
     *
     * @param caches Collection of cache names.
     */
    public CacheStatisticsClearMessage(UUID reqId, Collection<String> caches) {
        this.reqId = reqId;
        this.caches = caches;
        this.flags = INITIAL_MSG_MASK;
    }

    /**
     * Constructor for response.
     *
     * @param msg Request message.
     */
    private CacheStatisticsClearMessage(CacheStatisticsClearMessage msg) {
        this.reqId = msg.reqId;
        this.caches = null;
        this.flags = 0;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return this.id;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return initial() ? new CacheStatisticsClearMessage(this) : null;
    }

    /**
     * @return Cache names.
     */
    public Collection<String> caches() {
        return this.caches;
    }

    /**
     * Initial message flag.
     */
    public boolean initial() {
        return (flags & INITIAL_MSG_MASK) != 0;
    }

    /**
     * @return Request id.
     */
    public UUID requestId() {
        return this.reqId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheStatisticsClearMessage.class, this);
    }
}
