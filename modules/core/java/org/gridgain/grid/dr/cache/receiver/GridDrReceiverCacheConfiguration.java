/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.cache.receiver;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import static org.gridgain.grid.dr.cache.receiver.GridDrReceiverCacheConflictResolverMode.*;

/**
 * Data center replication receiver cache configuration.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridDrReceiverCacheConfiguration {
    /** Default data center replication receiver cache conflict resolver mode. */
    public static final GridDrReceiverCacheConflictResolverMode DFLT_CONFLICT_RSLVR_MODE = DR_AUTO;

    /** Receiver cache conflict resolver mode. */
    private GridDrReceiverCacheConflictResolverMode conflictRslvrMode = DFLT_CONFLICT_RSLVR_MODE;

    /** Receiver cache conflict resolver. */
    private GridDrReceiverCacheConflictResolver conflictRslvr;

    /**
     * Default constructor.
     */
    public GridDrReceiverCacheConfiguration() {
        // No-op.
    }

    /**
     * Copying constructor.
     *
     * @param cfg Configuration to copy.
     */
    public GridDrReceiverCacheConfiguration(GridDrReceiverCacheConfiguration cfg) {
        assert cfg != null;

        conflictRslvr = cfg.getConflictResolver();
        conflictRslvrMode = cfg.getConflictResolverMode();
    }

    /**
     * Gets conflict resolver mode. See {@link GridDrReceiverCacheConflictResolverMode} for more information.
     * <p>
     * Defaults to {@link #DFLT_CONFLICT_RSLVR_MODE}.
     *
     * @return Conflict resolution mode.
     */
    public GridDrReceiverCacheConflictResolverMode getConflictResolverMode() {
        return conflictRslvrMode;
    }

    /**
     * Sets conflict resolver mode. See {@link #getConflictResolverMode()} for
     * more information.
     *
     * @param conflictRslvrMode Conflict resolver mode.
     */
    public void setConflictResolverMode(GridDrReceiverCacheConflictResolverMode conflictRslvrMode) {
        this.conflictRslvrMode = conflictRslvrMode;
    }

    /**
     * Gets conflict resolver.
     * <p>
     * For {@link GridDrReceiverCacheConflictResolverMode#DR_AUTO} mode this parameter can be {@code null} and in
     * this case new entry will always overwrite the old one.
     * <p>
     * For {@link GridDrReceiverCacheConflictResolverMode#DR_ALWAYS} mode this parameter is mandatory.
     * <p>
     * Default value is {@code null}.
     *
     * @return Conflict resolver.
     */
    @Nullable public GridDrReceiverCacheConflictResolver getConflictResolver() {
        return conflictRslvr;
    }

    /**
     * Sets conflict resolver. See {@link #getConflictResolver()} for more
     * information.
     * <p>
     * Defaults to {@code null}.
     *
     * @param conflictRslvr Conflict resolver.
     */
    public void setConflictResolver(GridDrReceiverCacheConflictResolver conflictRslvr) {
        this.conflictRslvr = conflictRslvr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrReceiverCacheConfiguration.class, this);
    }
}
