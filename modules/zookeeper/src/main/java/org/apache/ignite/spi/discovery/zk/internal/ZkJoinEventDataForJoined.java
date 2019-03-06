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

package org.apache.ignite.spi.discovery.zk.internal;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class ZkJoinEventDataForJoined implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final List<ZookeeperClusterNode> top;

    /** */
    private final Map<Long, byte[]> discoData;

    /** */
    private final Map<Long, Long> dupDiscoData;

    /**
     * @param top Topology.
     * @param discoData Discovery data.
     */
    ZkJoinEventDataForJoined(List<ZookeeperClusterNode> top, Map<Long, byte[]> discoData, @Nullable Map<Long, Long> dupDiscoData) {
        assert top != null;
        assert discoData != null && !discoData.isEmpty();

        this.top = top;
        this.discoData = discoData;
        this.dupDiscoData = dupDiscoData;
    }

    byte[] discoveryDataForNode(long nodeOrder) {
        assert discoData != null;

        byte[] dataBytes = discoData.get(nodeOrder);

        if (dataBytes != null)
            return dataBytes;

        assert dupDiscoData != null;

        Long dupDataNode = dupDiscoData.get(nodeOrder);

        assert dupDataNode != null;

        dataBytes = discoData.get(dupDataNode);

        assert dataBytes != null;

        return dataBytes;
    }

    /**
     * @return Current topology.
     */
    List<ZookeeperClusterNode> topology() {
        assert top != null;

        return top;
    }
}
