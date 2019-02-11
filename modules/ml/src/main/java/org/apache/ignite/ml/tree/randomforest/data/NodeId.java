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
