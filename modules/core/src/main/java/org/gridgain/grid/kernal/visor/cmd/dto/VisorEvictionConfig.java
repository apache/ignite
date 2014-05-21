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
 * Eviction configuration data.
 */
public class VisorEvictionConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final String plc;
    private final int keyBufSize;
    private final boolean evictSynchronized;
    private final boolean nearSynchronized;
    private final float maxOverflowRatio;

    public VisorEvictionConfig(String plc, int keyBufSize, boolean evictSynchronized, boolean nearSynchronized,
        float maxOverflowRatio) {
        this.plc = plc;
        this.keyBufSize = keyBufSize;
        this.evictSynchronized = evictSynchronized;
        this.nearSynchronized = nearSynchronized;
        this.maxOverflowRatio = maxOverflowRatio;
    }

    /**
     * @return Policy.
     */
    public String policy() {
        return plc;
    }

    /**
     * @return Key buffer size.
     */
    public int keyBufferSize() {
        return keyBufSize;
    }

    /**
     * @return Evict synchronized.
     */
    public boolean evictSynchronized() {
        return evictSynchronized;
    }

    /**
     * @return Near synchronized.
     */
    public boolean nearSynchronized() {
        return nearSynchronized;
    }

    /**
     * @return Max overflow ratio.
     */
    public float maxOverflowRatio() {
        return maxOverflowRatio;
    }
}
