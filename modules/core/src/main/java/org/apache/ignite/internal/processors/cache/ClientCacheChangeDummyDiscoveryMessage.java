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

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy discovery message which is not really sent via ring, it is just added in local discovery worker queue.
 */
public class ClientCacheChangeDummyDiscoveryMessage implements DiscoveryCustomMessage,
    CachePartitionExchangeWorkerTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final UUID reqId;

    /** */
    private final Map<String, DynamicCacheChangeRequest> startReqs;

    /** */
    @GridToStringInclude
    private final Set<String> cachesToClose;

    /**
     * @param reqId Start request ID.
     * @param startReqs Caches start requests.
     * @param cachesToClose Cache to close.
     */
    public ClientCacheChangeDummyDiscoveryMessage(
        UUID reqId,
        @Nullable Map<String, DynamicCacheChangeRequest> startReqs,
        @Nullable Set<String> cachesToClose
    ) {
        assert reqId != null;
        assert startReqs != null ^ cachesToClose != null;

        this.reqId = reqId;
        this.startReqs = startReqs;
        this.cachesToClose = cachesToClose;
    }

    /** {@inheritDoc} */
    @Override public boolean skipForExchangeMerge() {
        return true;
    }

    /**
     * @return Start request ID.
     */
    UUID requestId() {
        return reqId;
    }

    /**
     * @return Cache start requests.
     */
    @Nullable Map<String, DynamicCacheChangeRequest> startRequests() {
        return startReqs;
    }

    /**
     * @return Client caches to close.
     */
    Set<String> cachesToClose() {
        return cachesToClose;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer, DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientCacheChangeDummyDiscoveryMessage.class, this,
            "startCaches", (startReqs != null ? startReqs.keySet() : ""));
    }
}
