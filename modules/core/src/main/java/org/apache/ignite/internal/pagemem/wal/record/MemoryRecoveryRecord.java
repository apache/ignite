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

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Marker indicates that binary memory recovery has finished.
 */
public class MemoryRecoveryRecord extends WALRecord {
    /** Create timestamp, millis */
    private final long time;

    /**
     * Default constructor.
     * @param time current timestamp, millis
     */
    public MemoryRecoveryRecord(long time) {
        this.time = time;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.MEMORY_RECOVERY;
    }

    /**
     * @return memory recovery start timestamp, millis
     */
    public long time() {
        return time;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MemoryRecoveryRecord.class, this, "super", super.toString());
    }
}
