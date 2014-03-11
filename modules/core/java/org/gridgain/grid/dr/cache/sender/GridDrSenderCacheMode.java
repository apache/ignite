/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.cache.sender;

import org.jetbrains.annotations.*;

/**
 * Cache data center replication sender cache mode.
 */
public enum GridDrSenderCacheMode {
    /**
     * Asynchronous mode. Transactions do not wait for acknowledge from sender hub before commit. Instead updat4d
     * data are put into shared batch. Once batch is too big or too old, it will be sent to the sender hub.
     * <p>
     * When there are too many cache entries enqueued for data center replication, ongoing transaction may block in
     * case total amount of batches awaiting acknowledge is greater than
     * {@link GridDrSenderCacheConfiguration#getMaxBatches()}.
     */
    DR_ASYNC;

    /** Enumerated values. */
    private static final GridDrSenderCacheMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridDrSenderCacheMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
