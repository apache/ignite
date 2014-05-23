/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import org.gridgain.grid.dr.cache.receiver.*;

import java.io.*;

/**
 * Visor counterpart for {@link GridDrReceiverCacheConfiguration}.
 */
public class VisorDrReceiverConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Conflict resolver */
    private final String conflictResolver;

    /** Conflict resolver mode. */
    private final GridDrReceiverCacheConflictResolverMode conflictResolverMode;

    public VisorDrReceiverConfig(String conflictResolver,
        GridDrReceiverCacheConflictResolverMode conflictResolverMode) {
        this.conflictResolver = conflictResolver;
        this.conflictResolverMode = conflictResolverMode;
    }

    /**
     * @return Conflict resolver
     */
    public String conflictResolver() {
        return conflictResolver;
    }

    /**
     * @return Conflict resolver mode.
     */
    public GridDrReceiverCacheConflictResolverMode conflictResolverMode() {
        return conflictResolverMode;
    }
}
