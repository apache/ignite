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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Checkpoint status which was restored from disk on node start.
 */
public class CheckpointStatus {
    /** Null checkpoint UUID. */
    public static final UUID NULL_UUID = new UUID(0L, 0L);

    /** Null WAL pointer. */
    public static final WALPointer NULL_PTR = new WALPointer(0, 0, 0);

    /** Checkpoint start timestamp. */
    public long cpStartTs;

    /** Checkpoint start id. */
    public UUID cpStartId;

    /** Checkpoint start pointer. */
    @GridToStringInclude
    public WALPointer startPtr;

    /** Checkpoint end id. */
    public UUID cpEndId;

    /** Checkpoint end pointer. */
    @GridToStringInclude
    public WALPointer endPtr;

    /**
     * @param cpStartId Checkpoint start ID.
     * @param startPtr Checkpoint start pointer.
     * @param cpEndId Checkpoint end ID.
     * @param endPtr Checkpoint end pointer.
     */
    CheckpointStatus(long cpStartTs, UUID cpStartId, WALPointer startPtr, UUID cpEndId, WALPointer endPtr) {
        this.cpStartTs = cpStartTs;
        this.cpStartId = cpStartId;
        this.startPtr = startPtr;
        this.cpEndId = cpEndId;
        this.endPtr = endPtr;
    }

    /**
     * @return {@code True} if need perform binary memory recovery. Only records {@link PageDeltaRecord} and {@link
     * PageSnapshot} needs to be applyed from {@link #cpStartId}.
     */
    public boolean needRestoreMemory() {
        return !Objects.equals(cpStartId, cpEndId) && !Objects.equals(NULL_UUID, cpStartId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CheckpointStatus.class, this);
    }
}
