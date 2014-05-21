/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

import java.util.*;

/**
 * Arguments for cache tasks.
 */
public class VisorOneNodeCachesArg extends VisorOneNodeArg {
    /** */
    private static final long serialVersionUID = 0L;

    /** Names of caches to clear. */
    private final Set<String> cacheNames;

    /**
     * @param nodeId Node Id.
     */
    public VisorOneNodeCachesArg(UUID nodeId, Set<String> cacheNames) {
        super(nodeId);

        this.cacheNames = cacheNames;
    }

    /**
     * @return Names of caches to clear.
     */
    public Set<String> cacheNames() {
        return cacheNames;
    }
}
