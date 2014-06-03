/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Data transfer object for near cache configuration properties.
 */
public class VisorNearCacheConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Flag to enable/disable near cache eviction policy. */
    private final boolean nearEnabled;

    /** Near cache start size. */
    private final int nearStartSize;

    /** Near cache eviction policy. */
    @Nullable private final String nearEvictPlc;

    /** Create data transfer object with given parameters. */
    public VisorNearCacheConfig(boolean nearEnabled, int nearStartSize, @Nullable String nearEvictPlc) {
        this.nearEnabled = nearEnabled;
        this.nearStartSize = nearStartSize;
        this.nearEvictPlc = nearEvictPlc;
    }

    /**
     * @return Flag to enable/disable near cache eviction policy.
     */
    public boolean nearEnabled() {
        return nearEnabled;
    }

    /**
     * @return Near cache start size.
     */
    public int nearStartSize() {
        return nearStartSize;
    }

    /**
     * @return Near cache eviction policy.
     */
    @Nullable public String nearEvictPolicy() {
        return nearEvictPlc;
    }
}
