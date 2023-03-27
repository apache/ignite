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

package org.apache.ignite.internal.sql.optimizer.affinity;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Flat group of partitions.
 */
public class PartitionGroupNode implements PartitionNode {
    /** Partitions. */
    @GridToStringInclude
    private final Set<PartitionSingleNode> siblings;

    /**
     * Constructor.
     *
     * @param siblings Partitions.
     */
    public PartitionGroupNode(Set<PartitionSingleNode> siblings) {
        assert !F.isEmpty(siblings);

        this.siblings = siblings;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionClientContext ctx, Object... args)
        throws IgniteCheckedException {
        // Deduplicate same partitions which may appear during resolution.
        HashSet<Integer> res = new HashSet<>(siblings.size());

        for (PartitionSingleNode sibling : siblings) {
            Integer part = sibling.applySingle(ctx, args);

            if (part == null)
                return null;

            res.add(part);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public int joinGroup() {
        // Note that we cannot cache join group in constructor. We have strong invariant that all siblings always
        // belongs to the same group. However, number of this group may be changed during expression tree traversing.
        return siblings.iterator().next().joinGroup();
    }

    /**
     * @return Siblings
     */
    public Set<PartitionSingleNode> siblings() {
        return siblings;
    }

    /**
     * Check if value exists. Should be called only on non-mixed node.
     *
     * @param val Value
     * @return {@code True} if exists.
     */
    public boolean contains(PartitionSingleNode val) {
        return siblings.contains(val);
    }

    /**
     * Check if current group node contains exactly the same set of siblings.
     *
     * @param siblings Siblings to check.
     * @return {@code True} if both sets of siblings contain the same elements.
     */
    public boolean containsExact(Collection<PartitionSingleNode> siblings) {
        return this.siblings.size() == siblings.size() && this.siblings.containsAll(siblings);
    }

    /**
     * @return {@code True} if the group contain only constants.
     */
    public boolean constantsOnly() {
        for (PartitionSingleNode sibling : siblings) {
            if (!sibling.constant())
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionGroupNode.class, this);
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return siblings.iterator().next().cacheName();
    }
}
