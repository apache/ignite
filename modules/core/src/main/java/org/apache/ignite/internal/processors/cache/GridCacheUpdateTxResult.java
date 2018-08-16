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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Cache entry transactional update result.
 */
public class GridCacheUpdateTxResult {
    /** Success flag.*/
    private final boolean success;

    /** Old value. */
    @GridToStringInclude
    private final CacheObject oldVal;

    /** Partition idx. */
    private long updateCntr = -1;

    /** */
    private WALPointer logPtr;

    /**
     * Constructor.
     *
     * @param success Success flag.
     * @param oldVal Old value (if any),
     * @param logPtr Logger WAL pointer for the update.
     */
    public GridCacheUpdateTxResult(boolean success, @Nullable CacheObject oldVal, WALPointer logPtr) {
        this.success = success;
        this.oldVal = oldVal;
        this.logPtr = logPtr;
    }

    /**
     * Constructor.
     *
     * @param success Success flag.
     * @param oldVal Old value (if any).
     * @param logPtr Logger WAL pointer for the update.
     */
    GridCacheUpdateTxResult(boolean success, @Nullable CacheObject oldVal, long updateCntr, WALPointer logPtr) {
        this.success = success;
        this.oldVal = oldVal;
        this.updateCntr = updateCntr;
        this.logPtr = logPtr;
    }

    /**
     * @return Partition idx.
     */
    public long updatePartitionCounter() {
        return updateCntr;
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return success;
    }

    /**
     * @return Logged WAL pointer for the update if persistence is enabled.
     */
    public WALPointer loggedPointer() {
        return logPtr;
    }

    /**
     * @return Old value.
     */
    @Nullable public CacheObject oldValue() {
        return oldVal;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheUpdateTxResult.class, this);
    }
}
