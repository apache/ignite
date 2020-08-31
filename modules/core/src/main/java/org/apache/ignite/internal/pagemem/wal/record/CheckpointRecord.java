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

package org.apache.ignite.internal.pagemem.wal.record;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Checkpoint (begin) record
 */
public class CheckpointRecord extends WALRecord {
    /** Checkpoint ID. */
    private UUID cpId;

    /** always false */
    private boolean end;

    /** */
    private Map<Integer, CacheState> cacheGrpStates;

    /** Safe replay pointer. */
    private WALPointer cpMark;

    /**
     * @param cpMark Checkpoint mark.
     */
    public CheckpointRecord(@Nullable WALPointer cpMark) {
        this(UUID.randomUUID(), cpMark);
    }

    /**
     * @param cpId Checkpoint ID.
     * @param cpMark Checkpoint mark.
     */
    public CheckpointRecord(UUID cpId, @Nullable WALPointer cpMark) {
        this.cpId = cpId;
        this.cpMark = cpMark;
    }

    /**
     * Constructor kept for serializer
     * @param cpId Checkpoint ID.
     * @param cpMark Checkpoint mark.
     * @param end Checkpoint end flag - deprecated expected to be always false
     */
    public CheckpointRecord(UUID cpId, @Nullable WALPointer cpMark, boolean end) {
        this.cpId = cpId;
        this.end = end;
        this.cpMark = cpMark;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CHECKPOINT_RECORD;
    }

    /**
     * @param grpId Cache group ID.
     * @param state Cache state.
     */
    public void addCacheGroupState(int grpId, CacheState state) {
        if (cacheGrpStates == null)
            cacheGrpStates = new HashMap<>();

        cacheGrpStates.put(grpId, state);
    }

    /**
     * @param cacheGrpStates Cache states.
     */
    public void cacheGroupStates(Map<Integer, CacheState> cacheGrpStates) {
        this.cacheGrpStates = cacheGrpStates;
    }

    /**
     * @return Cache states.
     */
    public Map<Integer, CacheState> cacheGroupStates() {
        return cacheGrpStates != null ? cacheGrpStates : Collections.<Integer, CacheState>emptyMap();
    }

    /**
     * @return Checkpoint ID.
     */
    public UUID checkpointId() {
        return cpId;
    }

    /**
     * @return Checkpoint end flag, now always false
     */
    public boolean end() {
        return end;
    }

    /**
     * @return Safe replay pointer.
     */
    public WALPointer checkpointMark() {
        return cpMark;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CheckpointRecord.class, this, "super", super.toString());
    }
}
