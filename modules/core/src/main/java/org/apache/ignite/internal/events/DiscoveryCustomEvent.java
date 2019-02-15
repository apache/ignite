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

package org.apache.ignite.internal.events;

import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotDiscoveryMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Custom event.
 */
public class DiscoveryCustomEvent extends DiscoveryEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Built-in event type: custom event sent.
     * <br>
     * Generated when someone invoke {@link GridDiscoveryManager#sendCustomEvent(DiscoveryCustomMessage)}.
     * <p>
     *
     * @see DiscoveryCustomEvent
     */
    public static final int EVT_DISCOVERY_CUSTOM_EVT = 18;

    /** */
    private DiscoveryCustomMessage customMsg;

    /** Affinity topology version. */
    private AffinityTopologyVersion affTopVer;

    /**
     * Default constructor.
     */
    public DiscoveryCustomEvent() {
        type(EVT_DISCOVERY_CUSTOM_EVT);
    }

    /**
     * @return Data.
     */
    public DiscoveryCustomMessage customMessage() {
        return customMsg;
    }

    /**
     * @param customMsg New customMessage.
     */
    public void customMessage(DiscoveryCustomMessage customMsg) {
        this.customMsg = customMsg;
    }

    /**
     * @return Affinity topology version.
     */
    public AffinityTopologyVersion affinityTopologyVersion() {
        return affTopVer;
    }

    /**
     * @param affTopVer Affinity topology version.
     */
    public void affinityTopologyVersion(AffinityTopologyVersion affTopVer) {
        this.affTopVer = affTopVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoveryCustomEvent.class, this, super.toString());
    }

    /**
     * @param evt Discovery event.
     * @return {@code True} if event is DiscoveryCustomEvent that requires centralized affinity assignment.
     */
    public static boolean requiresCentralizedAffinityAssignment(DiscoveryEvent evt) {
        if (!(evt instanceof DiscoveryCustomEvent))
            return false;

        return requiresCentralizedAffinityAssignment(((DiscoveryCustomEvent)evt).customMessage());
    }

    /**
     * @param msg Discovery custom message.
     * @return {@code True} if message belongs to event that requires centralized affinity assignment.
     */
    public static boolean requiresCentralizedAffinityAssignment(@Nullable DiscoveryCustomMessage msg) {
        if (msg == null)
            return false;

        if (msg instanceof ChangeGlobalStateMessage && ((ChangeGlobalStateMessage)msg).activate())
            return true;

        if (msg instanceof SnapshotDiscoveryMessage) {
            SnapshotDiscoveryMessage snapMsg = (SnapshotDiscoveryMessage) msg;

            return snapMsg.needExchange() && snapMsg.needAssignPartitions();
        }

        return false;
    }
}