/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    public ClientCacheChangeDummyDiscoveryMessage(UUID reqId,
        @Nullable Map<String, DynamicCacheChangeRequest> startReqs,
        @Nullable Set<String> cachesToClose) {
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
    @Override public boolean stopProcess() {
        return false;
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
