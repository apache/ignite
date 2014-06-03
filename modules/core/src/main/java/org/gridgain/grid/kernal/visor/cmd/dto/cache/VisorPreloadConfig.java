/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import org.gridgain.grid.cache.*;

import java.io.*;

/**
 * Data transfer object for cache preload configuration properties.
 */
public class VisorPreloadConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache preload mode. */
    private final GridCachePreloadMode mode;

    /** Preload pool size. */
    private final int poolSize;

    /** Cache preload batch size. */
    private final int batchSize;

    /** Preloading partitioned delay. */
    private final long partitionedDelay;

    /** Time in milliseconds to wait between preload messages. */
    private final long throttle;

    /** Preload timeout. */
    private final long timeout;

    /** Create data transfer object with given parameters. */
    public VisorPreloadConfig(
        GridCachePreloadMode mode,
        int poolSize,
        int batchSize,
        long partitionedDelay,
        long throttle,
        long timeout
    ) {
        this.mode = mode;
        this.poolSize = poolSize;
        this.batchSize = batchSize;
        this.partitionedDelay = partitionedDelay;
        this.throttle = throttle;
        this.timeout = timeout;
    }

    /**
     * @return Cache preload mode.
     */
    public GridCachePreloadMode mode() {
        return mode;
    }

    /**
     * @return Preload pool size.
     */
    public int poolSize() {
        return poolSize;
    }

    /**
     * @return Cache preload batch size.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * @return Partitioned delay.
     */
    public long partitionedDelay() {
        return partitionedDelay;
    }

    /**
     * @return Time in milliseconds to wait between preload messages.
     */
    public long throttle() {
        return throttle;
    }

    /**
     * @return Preload timeout.
     */
    public long timeout() {
        return timeout;
    }
}
