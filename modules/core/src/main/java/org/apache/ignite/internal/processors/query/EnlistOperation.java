/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    LOCK(null),
    /**
     * This operation applies entry transformer.
     */
    TRANSFORM(GridCacheOperation.UPDATE);

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

    /** */
    public boolean isInvoke() {
        return this == TRANSFORM;
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
