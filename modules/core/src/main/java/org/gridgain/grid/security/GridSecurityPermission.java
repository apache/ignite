/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.security;

import org.jetbrains.annotations.*;

/**
 * Supported security permissions within grid. Permissions
 * are specified on per-cache or per-task level.
 */
public enum GridSecurityPermission {
    /** Cache {@code read} permission. */
    CACHE_READ,

    /** Cache {@code put} permission. */
    CACHE_PUT,

    /** Cache {@code remove} permission. */
    CACHE_REMOVE,

    /** Task {@code execute} permission. */
    TASK_EXECUTE,

    /** Task {@code cancel} permission. */
    TASK_CANCEL,

    /** Events {@code enable} permission. */
    EVENTS_ENABLE,

    /** Events {@code disable} permission. */
    EVENTS_DISABLE,

    /** Common visor tasks permission. */
    ADMIN_VIEW,

    /** Visor cache read (query) permission. */
    ADMIN_QUERY,

    /** Visor cache load permission. */
    ADMIN_CACHE;

    /** Enumerated values. */
    private static final GridSecurityPermission[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridSecurityPermission fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
