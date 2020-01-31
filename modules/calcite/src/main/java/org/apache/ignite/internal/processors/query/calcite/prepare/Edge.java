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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import org.apache.calcite.rel.RelNode;

/**
 * Describes RelNode tree edge.
 */
public class Edge {
    /** */
    private final RelNode parent;

    /** */
    private final RelNode child;

    /** */
    private final int childIdx;

    /**
     * @param parent Parent node.
     * @param child Child node.
     * @param childIdx Child node index in parent.
     */
    public Edge(RelNode parent, RelNode child, int childIdx) {
        this.parent = parent;
        this.child = child;
        this.childIdx = childIdx;
    }

    /**
     * @return Parent node.
     */
    public RelNode parent() {
        return parent;
    }

    /**
     * @return Child node.
     */
    public RelNode child() {
        return child;
    }

    /**
     * @return Child node index in parent.
     */
    public int childIndex() {
        return childIdx;
    }
}
