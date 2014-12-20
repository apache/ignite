/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.transactions;

import org.jetbrains.annotations.*;

/**
 * Defines different cache transaction isolation levels. See {@link IgniteTx}
 * documentation for more information about cache transaction isolation levels.
 */
public enum GridCacheTxIsolation {
    /** Read committed isolation level. */
    READ_COMMITTED,

    /** Repeatable read isolation level. */
    REPEATABLE_READ,

    /** Serializable isolation level. */
    SERIALIZABLE;

    /** Enum values. */
    private static final GridCacheTxIsolation[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable
    public static GridCacheTxIsolation fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
