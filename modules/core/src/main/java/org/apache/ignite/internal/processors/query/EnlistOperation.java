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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.jetbrains.annotations.Nullable;

/**
 * Operations on entries which could be performed during transaction.
 * Operations are used during SQL statements execution, but does not define exact SQL statements semantics.
 * It is better to treat them independently and having their own semantics.
 */
public enum EnlistOperation {
    /**
     * This operation creates entry if it does not exist or raises visible failure otherwise.
     */
    INSERT(GridCacheOperation.CREATE),
    /**
     * This operation creates entry if it does not exist or modifies existing one otherwise.
     */
    UPSERT(GridCacheOperation.UPDATE),
    /**
     * This operation modifies existing entry or does nothing if entry does not exist.
     */
    UPDATE(GridCacheOperation.UPDATE),
    /**
     * This operation deletes existing entry or does nothing if entry does not exist.
     */
    DELETE(GridCacheOperation.DELETE),
    /**
     * This operation locks existing entry protecting it from updates by other transactions
     * or does notrhing if entry does not exist.
     */
    LOCK(null);

    /** */
    private final GridCacheOperation cacheOp;

    /** */
    EnlistOperation(GridCacheOperation cacheOp) {
        this.cacheOp = cacheOp;
    }

    /**
     * @return Corresponding Cache operation.
     */
    public GridCacheOperation cacheOperation() {
        return cacheOp;
    }

    /** */
    public boolean isDeleteOrLock() {
        return this == DELETE || this == LOCK;
    }

    /**
     * Indicates that an operation cannot create new row.
     */
    public boolean noCreate() {
        // has no meaning for LOCK
        assert this != LOCK;

        return this == UPDATE || this == DELETE;
    }

    /** Enum values. */
    private static final EnlistOperation[] VALS = values();

    /**
     * @param ord Ordinal value.
     * @return Enum value.
     */
    @Nullable public static EnlistOperation fromOrdinal(int ord) {
        return ord < 0 || ord >= VALS.length ? null : VALS[ord];
    }
}
