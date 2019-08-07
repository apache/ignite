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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager.exchangeProtocolVersion;

/**
 *
 */
public class ExchangeContext {
    /** */
    public static final String IGNITE_EXCHANGE_COMPATIBILITY_VER_1 = "IGNITE_EXCHANGE_COMPATIBILITY_VER_1";

    /** Cache groups to request affinity for during local join exchange. */
    private Set<Integer> requestGrpsAffOnJoin;

    /** Per-group affinity fetch on join (old protocol). */
    private boolean fetchAffOnJoin;

    /** Merges allowed flag. */
    private final boolean merge;

    /** */
    private final ExchangeDiscoveryEvents evts;

    /** */
    private final boolean compatibilityNode = getBoolean(IGNITE_EXCHANGE_COMPATIBILITY_VER_1, false);

    /**
     * @param crd Coordinator flag.
     * @param fut Exchange future.
     */
    public ExchangeContext(boolean crd, GridDhtPartitionsExchangeFuture fut) {
        int protocolVer = exchangeProtocolVersion(fut.firstEventCache().minimumNodeVersion());

        if (compatibilityNode || (crd && fut.localJoinExchange())) {
            fetchAffOnJoin = true;

            merge = false;
        }
        else {
            boolean startCaches = fut.exchangeId().isJoined() &&
                fut.sharedContext().cache().hasCachesReceivedFromJoin(fut.exchangeId().eventNode());

            fetchAffOnJoin = protocolVer == 1;

            merge = !startCaches &&
                protocolVer > 1 &&
                fut.firstEvent().type() != EVT_DISCOVERY_CUSTOM_EVT;
        }

        evts = new ExchangeDiscoveryEvents(fut);
    }

    /**
     * @param node Node.
     * @return {@code True} if node supports exchange merge protocol.
     */
    boolean supportsMergeExchanges(ClusterNode node) {
        return !compatibilityNode && exchangeProtocolVersion(node.version()) > 1;
    }

    /**
     * @return Discovery events.
     */
    public ExchangeDiscoveryEvents events() {
        return evts;
    }

    /**
     * @return {@code True} if on local join need fetch affinity per-group (old protocol),
     *      otherwise affinity is sent in {@link GridDhtPartitionsFullMessage}.
     */
    public boolean fetchAffinityOnJoin() {
        return fetchAffOnJoin;
    }

    /**
     * @param grpId Cache group ID.
     */
    synchronized void addGroupAffinityRequestOnJoin(Integer grpId) {
        if (requestGrpsAffOnJoin == null)
            requestGrpsAffOnJoin = new HashSet<>();

        requestGrpsAffOnJoin.add(grpId);
    }

    /**
     * @return Groups to request affinity for.
     */
    @Nullable public synchronized Set<Integer> groupsAffinityRequestOnJoin() {
        return requestGrpsAffOnJoin;
    }

    /**
     * @return {@code True} if exchanges merge is allowed during current exchange.
     */
    public boolean mergeExchanges() {
        return merge;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExchangeContext.class, this);
    }
}
