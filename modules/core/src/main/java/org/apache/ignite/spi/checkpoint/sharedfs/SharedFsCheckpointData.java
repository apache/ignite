/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.checkpoint.sharedfs;

import org.gridgain.grid.util.typedef.internal.*;
import java.io.*;

/**
 * Wrapper of all checkpoint that are saved to the file system. It
 * extends every checkpoint with expiration time and host name
 * which created this checkpoint.
 * <p>
 * Host name is used by {@link SharedFsCheckpointSpi} SPI to give node
 * correct files if it is restarted.
 */
class SharedFsCheckpointData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Checkpoint data. */
    private final byte[] state;

    /** Checkpoint expiration time. */
    private final long expTime;

    /** Host name that created this checkpoint. */
    private final String host;

    /** Checkpoint key. */
    private final String key;

    /**
     * Creates new instance of checkpoint data wrapper.
     *
     * @param state Checkpoint data.
     * @param expTime Checkpoint expiration time in milliseconds.
     * @param host Name of host that created this checkpoint.
     * @param key Key of checkpoint.
     */
    SharedFsCheckpointData(byte[] state, long expTime, String host, String key) {
        assert expTime >= 0;
        assert host != null;

        this.state = state;
        this.expTime = expTime;
        this.host = host;
        this.key = key;
    }

    /**
     * Gets checkpoint data.
     *
     * @return Checkpoint data.
     */
    byte[] getState() {
        return state;
    }

    /**
     * Gets checkpoint expiration time.
     *
     * @return Expire time in milliseconds.
     */
    long getExpireTime() {
        return expTime;
    }

    /**
     * Gets checkpoint host name.
     *
     * @return Host name.
     */
    String getHost() {
        return host;
    }

    /**
     * Gets key of checkpoint.
     *
     * @return Key of checkpoint.
     */
    public String getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SharedFsCheckpointData.class, this);
    }
}
