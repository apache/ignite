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

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition extraction result.
 */
public class PartitionResult {
    /** Tree. */
    @GridToStringInclude
    private final PartitionNode tree;

    /** Affinity function. */
    private final PartitionTableAffinityDescriptor aff;

    /**
     * Constructor.
     *
     * @param tree Tree.
     * @param aff Affinity function.
     */
    public PartitionResult(PartitionNode tree, PartitionTableAffinityDescriptor aff) {
        this.tree = tree;
        this.aff = aff;
    }

    /**
     * Tree.
     */
    public PartitionNode tree() {
        return tree;
    }

    /**
     * @return Affinity function.
     */
    public PartitionTableAffinityDescriptor affinity() {
        return aff;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionResult.class, this);
    }
}
