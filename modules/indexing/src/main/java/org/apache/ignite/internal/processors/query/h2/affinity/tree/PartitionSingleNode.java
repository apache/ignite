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
import java.util.Collections;

/**
 * Node with a single partition.
 */
public abstract class PartitionSingleNode implements PartitionNode {
    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionResolver resolver, Object... args) {
        return Collections.singletonList(applySingle(resolver, args));
    }

    /**
     * Apply arguments and get single partition.
     *
     * @param args Arguments.
     * @return Partition.
     */
    public abstract int applySingle(PartitionResolver resolver, Object... args);

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
        return (constant() ? 1 : 0) + value();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (obj.getClass() != getClass())
            return false;

        PartitionSingleNode other = (PartitionSingleNode)obj;

        return value() == other.value();
    }
}
