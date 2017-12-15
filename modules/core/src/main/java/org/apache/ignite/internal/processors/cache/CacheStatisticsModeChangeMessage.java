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

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** Request id. */
    private final UUID reqId;

    /** Cache names. */
    private final Collection<String> caches;

    /** Statistic enabled. */
    private final boolean enabled;

    /** Success. */
    private boolean success;

    /**
     * @param caches Collection of cache names.
     */
    public CacheStatisticsModeChangeMessage(UUID reqId, Collection<String> caches, boolean enabled) {
        this.reqId = reqId;
        this.caches = Collections.unmodifiableCollection(caches);
        this.enabled = enabled;
        this.success = true;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return new CacheStatisticsModeChangeResponse(reqId, success);
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
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
        return enabled;
    }

    /**
     * Sets success flag.
     */
    public void success(boolean success) {
        this.success = success;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheStatisticsModeChangeMessage.class, this);
    }
}
