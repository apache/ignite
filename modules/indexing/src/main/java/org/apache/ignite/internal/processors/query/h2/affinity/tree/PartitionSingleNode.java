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

import org.apache.ignite.internal.util.typedef.F;

import java.util.Collection;
import java.util.Collections;

/**
 * Node with a single partition.
 */
public abstract class PartitionSingleNode implements PartitionNode {
    /** Table descriptor. */
    protected final PartitionTableDescriptor tbl;

    /**
     * Constructor.
     *
     * @param tbl Table descriptor.
     */
    protected PartitionSingleNode(PartitionTableDescriptor tbl) {
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(Object... args) {
        return Collections.singletonList(applySingle(args));
    }

    /**
     * Apply arguments and get single partition.
     *
     * @param args Arguments.
     * @return Partition.
     */
    public abstract int applySingle(Object... args);

    /**
     * @return {@code True} if constant, {@code false} if argument.
     */
    public abstract boolean constant();

    /**
     * @return Partition for constant node, index for argument node.
     */
    public abstract int value();

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int hash = (constant() ? 1 : 0);

        hash = 31 * hash + value();
        hash = 31 * hash + tbl.hashCode();

        return hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (obj.getClass() != getClass())
            return false;

        PartitionSingleNode other = (PartitionSingleNode)obj;

        return F.eq(constant(), other.constant()) && F.eq(value(), other.value()) && F.eq(tbl, other.tbl);
    }
}
