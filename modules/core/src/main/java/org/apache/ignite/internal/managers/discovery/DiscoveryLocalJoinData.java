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
