/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import java.io.*;

/**
 * Near cache configuration data.
 */
public class VisorNearCacheConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final boolean nearEnabled;
    private final int nearStartSize;
    private final String nearEvictPlc;

    public VisorNearCacheConfig(boolean nearEnabled, int nearStartSize, String nearEvictPlc) {
        this.nearEnabled = nearEnabled;
        this.nearStartSize = nearStartSize;
        this.nearEvictPlc = nearEvictPlc;
    }

    /**
     * @return Near enabled.
     */
    public boolean nearEnabled() {
        return nearEnabled;
    }

    /**
     * @return Near start size.
     */
    public int nearStartSize() {
        return nearStartSize;
    }

    /**
     * @return Near evict policy.
     */
    public String nearEvictPolicy() {
        return nearEvictPlc;
    }
}
