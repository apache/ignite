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

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Information about local join event.
 */
public class DiscoveryLocalJoinData {
    /** */
    private final DiscoveryEvent evt;

    /** */
    private final DiscoCache discoCache;

    /** */
    private final AffinityTopologyVersion joinTopVer;

    /** */
    private final IgniteInternalFuture<Boolean> transitionWaitFut;

    /** */
    private final boolean active;

    /**
     * @param evt Event.
     * @param discoCache Discovery data cache.
     * @param transitionWaitFut Future if cluster state transition is in progress.
     * @param active Cluster active status.
     */
    public DiscoveryLocalJoinData(DiscoveryEvent evt,
        DiscoCache discoCache,
        @Nullable IgniteInternalFuture<Boolean> transitionWaitFut,
        boolean active) {
        assert evt != null && evt.topologyVersion() > 0 : evt;

        this.evt = evt;
        this.discoCache = discoCache;
        this.transitionWaitFut = transitionWaitFut;
        this.active = active;

        joinTopVer = new AffinityTopologyVersion(evt.topologyVersion(), 0);
    }

    /**
     * @return Future if cluster state transition is in progress.
     */
    @Nullable public IgniteInternalFuture<Boolean> transitionWaitFuture() {
        return transitionWaitFut;
    }

    /**
     * @return Cluster state.
     */
    public boolean active() {
        return active;
    }

    /**
     * @return Event.
     */
    public DiscoveryEvent event() {
        return evt;
    }

    /**
     * @return Discovery data cache.
     */
    public DiscoCache discoCache() {
        return discoCache;
    }

    /**
     * @return Join topology version.
     */
    public AffinityTopologyVersion joinTopologyVersion() {
        return joinTopVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoveryLocalJoinData.class, this);
    }
}
