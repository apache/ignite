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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Sent from cache client node to asynchronously notify about started.closed client caches.
 */
public class ClientCacheChangeDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    @GridToStringInclude
    private Map<Integer, Boolean> startedCaches;

    /** */
    @GridToStringInclude
    private Set<Integer> closedCaches;

    /** Update timeout object, used to batch multiple starts/close into single discovery message. */
    private transient ClientCacheUpdateTimeout updateTimeoutObj;

    /**
     * @param startedCaches Started caches.
     * @param closedCaches Closed caches.
     */
    public ClientCacheChangeDiscoveryMessage(Map<Integer, Boolean> startedCaches, Set<Integer> closedCaches) {
        this.startedCaches = startedCaches;
        this.closedCaches = closedCaches;
    }

    /**
     * @param startedCaches Started caches.
     * @param closedCaches Closed caches.
     */
    public void merge(@Nullable Map<Integer, Boolean> startedCaches, @Nullable Set<Integer> closedCaches) {
        Map<Integer, Boolean> startedCaches0 = this.startedCaches;
        Set<Integer> closedCaches0 = this.closedCaches;

        if (startedCaches != null) {
            if (startedCaches0 == null)
                this.startedCaches = startedCaches0 = new HashMap<>();

            for (Map.Entry<Integer, Boolean> e : startedCaches.entrySet()) {
                if (closedCaches0 != null && closedCaches0.remove(e.getKey()))
                    continue;

                Boolean old = startedCaches0.put(e.getKey(), e.getValue());

                assert old == null : e.getKey();
            }
        }

        if (closedCaches != null) {
            if (closedCaches0 == null)
                this.closedCaches = closedCaches0 = new HashSet<>();

            for (Integer cacheId : closedCaches) {
                if (startedCaches0 != null && startedCaches0.remove(cacheId) != null)
                    continue;

                boolean add = closedCaches0.add(cacheId);

                assert add : cacheId;
            }
        }
    }

    /**
     * @return {@code True} if there are no info about started/closed caches.
     */
    public boolean empty() {
        return F.isEmpty(startedCaches) && F.isEmpty(closedCaches);
    }

    /**
     * @param caches Started caches' IDs.
     */
    void checkCachesExist(Set<Integer> caches) {
        if (closedCaches != null) {
            for (Iterator<Integer> it = closedCaches.iterator(); it.hasNext();) {
                Integer cacheId = it.next();

                if (!caches.contains(cacheId))
                    it.remove();
            }
        }

        if (startedCaches != null) {
            for (Iterator<Integer> it = startedCaches.keySet().iterator(); it.hasNext();) {
                Integer cacheId = it.next();

                if (!caches.contains(cacheId))
                    it.remove();
            }
        }
    }

    /**
     * @return Update timeout object.
     */
    public ClientCacheUpdateTimeout updateTimeoutObject() {
        return updateTimeoutObj;
    }

    /**
     * @param updateTimeoutObj Update timeout object.
     */
    public void updateTimeoutObject(ClientCacheUpdateTimeout updateTimeoutObj) {
        this.updateTimeoutObj = updateTimeoutObj;
    }

    /**
     * @return Started caches map (cache ID to near enabled flag).
     */
    @Nullable public Map<Integer, Boolean> startedCaches() {
        return startedCaches;
    }

    /**
     * @return Closed caches.
     */
    @Nullable public Set<Integer> closedCaches() {
        return closedCaches;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientCacheChangeDiscoveryMessage.class, this);
    }
}
