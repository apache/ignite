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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cache change batch.
 */
public class DynamicCacheChangeBatch implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Change requests. */
    @GridToStringInclude
    private Collection<DynamicCacheChangeRequest> reqs;

    /** Client nodes map. Used in discovery data exchange. */
    @GridToStringInclude
    private Map<String, Map<UUID, Boolean>> clientNodes;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private boolean clientReconnect;

    /** */
    private boolean startCaches;

    /**
     * @param reqs Requests.
     */
    public DynamicCacheChangeBatch(
        Collection<DynamicCacheChangeRequest> reqs
    ) {
        this.reqs = reqs;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     * @param id Message ID.
     */
    public void id(IgniteUuid id) {
        this.id = id;
    }

    /**
     * @return Collection of change requests.
     */
    public Collection<DynamicCacheChangeRequest> requests() {
        return reqs;
    }

    /**
     * @return Client nodes map.
     */
    public Map<String, Map<UUID, Boolean>> clientNodes() {
        return clientNodes;
    }

    /**
     * @param clientNodes Client nodes map.
     */
    public void clientNodes(Map<String, Map<UUID, Boolean>> clientNodes) {
        this.clientNodes = clientNodes;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /**
     * @param clientReconnect {@code True} if this is discovery data sent on client reconnect.
     */
    public void clientReconnect(boolean clientReconnect) {
        this.clientReconnect = clientReconnect;
    }

    /**
     * @return {@code True} if this is discovery data sent on client reconnect.
     */
    public boolean clientReconnect() {
        return clientReconnect;
    }

    /**
     * @return {@code True} if required to start all caches on client node.
     */
    public boolean startCaches() {
        return startCaches;
    }

    /**
     * @param startCaches {@code True} if required to start all caches on client node.
     */
    public void startCaches(boolean startCaches) {
        this.startCaches = startCaches;
    }

    /**
     * @return {@code True} if request should trigger partition exchange.
     */
    public boolean exchangeNeeded() {
        if (reqs != null) {
            for (DynamicCacheChangeRequest req : reqs) {
                if (req.exchangeNeeded())
                    return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheChangeBatch.class, this);
    }
}