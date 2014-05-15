/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

/**
 * GGFS client future that holds response parse closure.
 */
public class GridGgfsHadoopFuture<T> extends GridPlainFutureAdapter<T> {
    /** Output buffer. */
    private byte[] outBuf;

    /** Output offset. */
    private int outOff;

    /** Output length. */
    private int outLen;

    /** Read future flag. */
    private boolean read;

    /**
     * @return Output buffer.
     */
    public byte[] outputBuffer() {
        return outBuf;
    }

    /**
     * @param outBuf Output buffer.
     */
    public void outputBuffer(@Nullable byte[] outBuf) {
        this.outBuf = outBuf;
    }

    /**
     * @return Offset in output buffer to write from.
     */
    public int outputOffset() {
        return outOff;
    }

    /**
     * @param outOff Offset in output buffer to write from.
     */
    public void outputOffset(int outOff) {
        this.outOff = outOff;
    }

    /**
     * @return Length to write to output buffer.
     */
    public int outputLength() {
        return outLen;
    }

    /**
     * @param outLen Length to write to output buffer.
     */
    public void outputLength(int outLen) {
        this.outLen = outLen;
    }

    /**
     * @param read {@code True} if this is a read future.
     */
    public void read(boolean read) {
        this.read = read;
    }

    /**
     * @return {@code True} if this is a read future.
     */
    public boolean read() {
        return read;
    }
}
