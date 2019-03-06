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

package org.apache.ignite.ml.tree.randomforest.data;

import java.io.Serializable;
import java.util.Objects;

/**
 * Class represents Node id in Random Forest consisting of tree id and node id in tree in according to
 * breadth-first search in tree.
 */
public class NodeId implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 4400852013136423333L;

    /** Tree id. */
    private final int treeId;

    /** Node id. */
    private final long nodeId;

    /**
     * Create an instance of NodeId.
     *
     * @param treeId Tree id.
     * @param nodeId Node id.
     */
    public NodeId(int treeId, long nodeId) {
        this.treeId = treeId;
        this.nodeId = nodeId;
    }

    /**
     *
     * @return Tree id.
     */
    public int treeId() {
        return treeId;
    }

    /**
     *
     * @return Node id.
     */
    public long nodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        NodeId id = (NodeId)o;
        return Objects.equals(treeId, id.treeId) &&
            Objects.equals(nodeId, id.nodeId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(treeId, nodeId);
    }
}
