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
import java.util.HashSet;

/**
 * Flat group of partitions.
 */
public class PartitionGroupNode implements PartitionNode {
    /** Partitions. */
    private final Collection<PartitionSingleNode> siblings;

    /**
     * Constructor.
     *
     * @param siblings Partitions.
     */
    public PartitionGroupNode(Collection<PartitionSingleNode> siblings) {
        assert !F.isEmpty(siblings);

        this.siblings = siblings;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionResolver resolver, Object... args) {
        // Deduplicate same partitions which may appear during resolution.
        HashSet<Integer> res = new HashSet<>(siblings.size());

        for (PartitionSingleNode sibling : siblings)
            res.add(sibling.applySingle(resolver, args));

        return res;
    }
}
