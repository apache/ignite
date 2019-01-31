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
import java.util.Collections;
import java.util.IdentityHashMap;

/**
 * Group of joined tables whose affinity function could be "merged".
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class PartitionJoinGroup {
    /** Tables within a group. */
    private final Collection<PartitionTable> tbls = Collections.newSetFromMap(new IdentityHashMap<>());

    /** Affinity function descriptor. */
    private final PartitionTableAffinityDescriptor affDesc;

    /**
     * Constructor.
     *
     * @param affDesc Affinity function descriptor.
     */
    public PartitionJoinGroup(PartitionTableAffinityDescriptor affDesc) {
        this.affDesc = affDesc;
    }

    /**
     * @return Tables in a group.
     */
    public Collection<PartitionTable> tables() {
        return tbls;
    }

    /**
     * Add table to the group.
     *
     * @param tbl Table.
     * @return This for chaining.
     */
    public PartitionJoinGroup addTable(PartitionTable tbl) {
        tbls.add(tbl);

        return this;
    }

    /**
     * Remove table from the group.
     *
     * @param tbl Table.
     * @return If group is empty after removal.
     */
    public boolean removeTable(PartitionTable tbl) {
        tbls.remove(tbl);

        return tbls.isEmpty();
    }

    /**
     * @return Affinity descriptor.
     */
    public PartitionTableAffinityDescriptor affinityDescriptor() {
        return affDesc;
    }
}
