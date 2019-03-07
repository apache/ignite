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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** */
public class CompoundSnapshotOperation implements SnapshotOperation {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * The list of operations. Taking snapshot operation by {@link IgniteCacheSnapshotManager}
     * will be always placed as the head of the list.
     */
    private final List<SnapshotOperation> ops = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public Set<Integer> cacheGroupIds() {
        return ops.stream()
            .map(SnapshotOperation::cacheGroupIds)
            .collect(HashSet::new, Set::addAll, Set::addAll);
    }

    /** {@inheritDoc} */
    @Override public Set<String> cacheNames() {
        return ops.stream()
            .map(SnapshotOperation::cacheNames)
            .collect(HashSet::new, Set::addAll, Set::addAll);
    }

    /** {@inheritDoc} */
    @Override public Object extraParameter() {
        return ops.get(0).extraParameter();
    }

    /**
     * @param op Snapshot operation to add.
     * @param top {@code True} to add operation to the head of the list.
     */
    public void addSnapshotOperation(SnapshotOperation op, boolean top) {
        if (top)
            ops.add(0, op); // Other elements will be shifted to the right.
        else
            ops.add(op);
    }
}
