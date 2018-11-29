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

package org.apache.ignite.internal.processors.query.h2.affinity.tree;

import java.util.Collection;

/**
 * Composite node which consists of two child nodes and a relation between them.
 */
public class PartitionCompositeNode implements PartitionNode {
    /** Left node. */
    private final PartitionNode left;

    /** Right node. */
    private final PartitionNode right;

    /** Operator. */
    private final PartitionCompositeNodeOperator op;

    /**
     * Constructor.
     *
     * @param left Left node.
     * @param right Right node.
     * @param op Operator.
     */
    public PartitionCompositeNode(PartitionNode left, PartitionNode right, PartitionCompositeNodeOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionResolver resolver, Object... args) {
        Collection<Integer> leftParts = left.apply(resolver, args);
        Collection<Integer> rightParts = right.apply(resolver, args);

        if (op == PartitionCompositeNodeOperator.AND)
            leftParts.retainAll(rightParts);
        else {
            assert op == PartitionCompositeNodeOperator.OR;

            leftParts.addAll(rightParts);
        }

        return leftParts;
    }

    /**
     * Try optimizing partition nodes into a simpler form.
     *
     * @return Optimized node or {@code this} if optimization failed.
     */
    public PartitionNode optimize() {
        // TODO
        return this;
    }
}
