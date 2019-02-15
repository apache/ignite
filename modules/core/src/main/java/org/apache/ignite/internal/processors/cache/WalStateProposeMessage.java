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

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

import java.util.Map;
import java.util.UUID;

/**
 * WAL state propose message.
 */
public class WalStateProposeMessage extends WalStateAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private final UUID nodeId;

    /** Cache names which are expected to be in the group along with their deployment IDs. */
    private Map<String, IgniteUuid> caches;

    /** Whether WAL should be enabled or disabled. */
    private final boolean enable;

    /** Whether message is being handled on cache affinity node. */
    private transient boolean affNode;

    /**
     * Constructor.
     *
     * @param opId Operation IDs.
     * @param grpId Expected group ID.
     * @param grpDepId Expected group deployment ID.
     * @param nodeId Node ID.
     * @param caches Expected cache names and their relevant deployment IDs.
     *
     * @param enable WAL state flag.
     */
    public WalStateProposeMessage(UUID opId, int grpId, IgniteUuid grpDepId, UUID nodeId,
        Map<String, IgniteUuid> caches, boolean enable) {
        super(opId, grpId, grpDepId);

        this.nodeId = nodeId;
        this.caches = caches;
        this.enable = enable;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Caches.
     */
    public Map<String, IgniteUuid> caches() {
        return caches;
    }

    /**
     * @return WAL state flag.
     */
    public boolean enable() {
        return enable;
    }

    /**
     * @return Whether message is being handled on cache affintiy node.
     */
    public boolean affinityNode() {
        return affNode;
    }

    /**
     * @param affNode Whether message is being handled on cache affintiy node.
     */
    public void affinityNode(boolean affNode) {
        this.affNode = affNode;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalStateProposeMessage.class, this, "super", super.toString());
    }
}
