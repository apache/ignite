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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collections;
import java.util.Set;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.jetbrains.annotations.Nullable;

/**
 * Represents result of {@link SnapshotFutureTask}.
 */
public class SnapshotFutureTaskResult {
    /** Partitions for which snapshot was created. */
    private final Set<GroupPartitionId> parts;

    /** Pointer to {@link ClusterSnapshotRecord} in WAL. */
    private final @Nullable WALPointer snpPtr;

    /** */
    public SnapshotFutureTaskResult(Set<GroupPartitionId> parts, @Nullable WALPointer snpPtr) {
        this.parts = Collections.unmodifiableSet(parts);
        this.snpPtr = snpPtr;
    }

    /** */
    Set<GroupPartitionId> parts() {
        return parts;
    }

    /** */
    @Nullable WALPointer snapshotPointer() {
        return snpPtr;
    }
}
