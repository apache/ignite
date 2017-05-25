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

import org.apache.ignite.internal.pagemem.wal.WALPointer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public class CheckpointRecord extends WALRecord {
    /** Checkpoint ID. */
    private UUID cpId;

    /** */
    private boolean end;

    /** */
    private Map<Integer, CacheState> cacheStates;

    /** Safe replay pointer. */
    private WALPointer cpMark;

    /**
     * @param cpMark Checkpoint mark.
     * @param end Checkpoint end flag.
     */
    public CheckpointRecord(WALPointer cpMark, boolean end) {
        this(UUID.randomUUID(), cpMark, end);
    }

    /**
     * @param cpId Checkpoint ID.
     * @param cpMark Checkpoint mark.
     * @param end Checkpoint end flag.
     */
    public CheckpointRecord(UUID cpId, WALPointer cpMark, boolean end) {
        this.cpId = cpId;
        this.end = end;
        this.cpMark = cpMark;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CHECKPOINT_RECORD;
    }

    /**
     * @param cacheId Cache ID.
     * @param state Cache state.
     */
    public void addCacheState(int cacheId, CacheState state) {
        if (cacheStates == null)
            cacheStates = new HashMap<>();

        cacheStates.put(cacheId, state);
    }

    /**
     * @param cacheStates Cache states.
     */
    public void cacheStates(Map<Integer, CacheState> cacheStates) {
        this.cacheStates = cacheStates;
    }

    /**
     * @return Cache states.
     */
    public Map<Integer, CacheState> cacheStates() {
        return cacheStates != null ? cacheStates : Collections.<Integer, CacheState>emptyMap();
    }

    /**
     * @return Checkpoint ID.
     */
    public UUID checkpointId() {
        return cpId;
    }

    /**
     * @return Checkpoint end flag.
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
}
