/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import org.gridgain.grid.dr.cache.receiver.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

import static org.gridgain.grid.kernal.visor.cmd.VisorTaskUtils.*;

/**
 * Data transfer object for DR receiver cache configuration properties.
 */
public class VisorDrReceiverConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Conflict resolver */
    private String conflictResolver;

    /** Conflict resolver mode. */
    private GridDrReceiverCacheConflictResolverMode conflictResolverMode;

    /**
     * @param rcvCfg Data center replication receiver cache configuration.
     * @return Data transfer object for DR receiver cache configuration properties.
     */
    public static VisorDrReceiverConfig from(GridDrReceiverCacheConfiguration rcvCfg) {
        VisorDrReceiverConfig cfg = new VisorDrReceiverConfig();

        cfg.conflictResolver(compactClass(rcvCfg.getConflictResolver()));
        cfg.conflictResolverMode(rcvCfg.getConflictResolverMode());

        return cfg;
    }

    /**
     * @return Conflict resolver
     */
    public String conflictResolver() {
        return conflictResolver;
    }

    /**
     * @param conflictRslvr New conflict resolver
     */
    public void conflictResolver(String conflictRslvr) {
        conflictResolver = conflictRslvr;
    }

    /**
     * @return Conflict resolver mode.
     */
    public GridDrReceiverCacheConflictResolverMode conflictResolverMode() {
        return conflictResolverMode;
    }

    /**
     * @param conflictRslvrMode New conflict resolver mode.
     */
    public void conflictResolverMode(GridDrReceiverCacheConflictResolverMode conflictRslvrMode) {
        conflictResolverMode = conflictRslvrMode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrReceiverConfig.class, this);
    }
}
