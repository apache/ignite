/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.cache.*;

import java.io.*;

/**
 * Preload configuration data.
 */
public class VisorPreloadConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final GridCachePreloadMode mode;
    private final int poolSize;
    private final int batchSize;

    public VisorPreloadConfig(GridCachePreloadMode mode, int poolSize, int batchSize) {
        this.mode = mode;
        this.poolSize = poolSize;
        this.batchSize = batchSize;
    }

    /**
     * @return Mode.
     */
    public GridCachePreloadMode mode() {
        return mode;
    }

    /**
     * @return Pool size.
     */
    public int poolSize() {
        return poolSize;
    }

    /**
     * @return Batch size.
     */
    public int batchSize() {
        return batchSize;
    }
}
