package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.jetbrains.annotations.*;

import java.util.concurrent.*;

/**
 * Enumeration of all queue operation, which have common implementation and need operation name for logging.
 */
enum GridCacheQueueOperation {
    /** {@link GridCacheQueueImpl#addx(Object)}*/
    ADD,

    /** {@link GridCacheQueueImpl#put(Object)}*/
    PUT,

    /** {@link GridCacheQueueImpl#put(Object, long, TimeUnit)}*/
    PUT_TIMEOUT,

    /** {@link GridCacheQueueImpl#get()}*/
    GET,

    /** {@link GridCacheQueueImpl#get(long, TimeUnit)}*/
    GET_TIMEOUT,

    /** {@link GridCacheQueueImpl#getLast()}*/
    GET_LAST,

    /** {@link GridCacheQueueImpl#getLast(long, TimeUnit)}*/
    GET_LAST_TIMEOUT,

    /** {@link GridCacheQueueImpl#take()}*/
    TAKE,

    /** {@link GridCacheQueueImpl#take(long, TimeUnit)}*/
    TAKE_TIMEOUT,

    /** {@link GridCacheQueueImpl#takeLast()}*/
    TAKE_LAST,

    /** {@link GridCacheQueueImpl#takeLast(long, TimeUnit)}*/
    TAKE_LAST_TIMEOUT;

    /** Enumerated values. */
    private static final GridCacheQueueOperation[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridCacheQueueOperation fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
