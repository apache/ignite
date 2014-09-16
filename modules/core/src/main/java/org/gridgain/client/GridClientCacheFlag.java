/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.gridgain.grid.portables.*;

import java.util.*;

/**
 * Cache projection flags that specify projection behaviour.
 */
public enum GridClientCacheFlag {
    /** Skips store, i.e. no read-through and no write-through behavior. */
    SKIP_STORE,

    /** Skip swap space for reads and writes. */
    SKIP_SWAP,

    /** Synchronous commit. */
    SYNC_COMMIT,

    /**
     * Switches a cache projection to work in {@code 'invalidation'} mode.
     * Instead of updating remote entries with new values, small invalidation
     * messages will be sent to set the values to {@code null}.
     */
    INVALIDATE,

    /**
     * Disable deserialization of portable objects on get operations.
     * If set and portable marshaller is used, {@link GridClientData#get(Object)}
     * and {@link GridClientData#getAll(Collection)} methods will return
     * instances of {@link GridPortableObject} class instead of user objects.
     * Use this flag if you don't have corresponding class on your client of
     * if you want to get access to some individual fields, but do not want to
     * fully deserialize the object.
     */
    KEEP_PORTABLES;

    /** */
    private static final GridClientCacheFlag[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    public static GridClientCacheFlag fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
