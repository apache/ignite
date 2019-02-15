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
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
class ZkJoiningNodeData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int partCnt;

    /** */
    @GridToStringInclude
    private ZookeeperClusterNode node;

    /** */
    @GridToStringInclude
    private Map<Integer, Serializable> discoData;

    /**
     * @param partCnt Number of parts in multi-parts message.
     */
    ZkJoiningNodeData(int partCnt) {
        this.partCnt = partCnt;
    }

    /**
     * @param node Node.
     * @param discoData Discovery data.
     */
    ZkJoiningNodeData(ZookeeperClusterNode node, Map<Integer, Serializable> discoData) {
        assert node != null && node.id() != null : node;
        assert discoData != null;

        this.node = node;
        this.discoData = discoData;
    }

    /**
     * @return Number of parts in multi-parts message.
     */
    int partCount() {
        return partCnt;
    }

    /**
     * @return Node.
     */
    ZookeeperClusterNode node() {
        return node;
    }

    /**
     * @return Discovery data.
     */
    Map<Integer, Serializable> discoveryData() {
        return discoData;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ZkJoiningNodeData.class, this);
    }
}
