/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.product;

import org.jetbrains.annotations.*;

/**
 * Different GridGain editions.
 */
public enum GridProductEdition {
    /** In-Memory HPC. */
    HPC,

    /** In-Memory Data Grid. */
    DATA_GRID,

    /** In-Memory Streaming. */
    STREAMING,

    /** In-Memory Accelerator For Hadoop. */
    HADOOP,

    /** In-Memory Accelerator For MongoDB. */
    MONGO,

    /** Platform edition which contains all functionality from other editions. */
    PLATFORM;

    /** Enumerated values. */
    private static final GridProductEdition[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridProductEdition fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
