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

import java.io.*;

/**
 * Extended GGFS server interface.
 */
public interface GridGgfsHadoopEx extends GridGgfsHadoop {
    /**
     * Adds event listener that will be invoked when connection with server is lost or remote error has occurred.
     * If connection is closed already, callback will be invoked synchronously inside this method.
     *
     * @param delegate Stream delegate.
     * @param lsnr Event listener.
     */
    public void addEventListener(GridGgfsHadoopStreamDelegate delegate, GridGgfsHadoopStreamEventListener lsnr);

    /**
     * Removes event listener that will be invoked when connection with server is lost or remote error has occurred.
     *
     * @param delegate Stream delegate.
     */
    public void removeEventListener(GridGgfsHadoopStreamDelegate delegate);

    /**
     * Asynchronously reads specified amount of bytes from opened input stream.
     *
     * @param delegate Stream delegate.
     * @param pos Position to read from.
     * @param len Data length to read.
     * @param outBuf Optional output buffer. If buffer length is less then {@code len}, all remaining
     *     bytes will be read into new allocated buffer of length {len - outBuf.length} and this buffer will
     *     be the result of read future.
     * @param outOff Output offset.
     * @param outLen Output length.
     * @return Read data.
     */
    public GridPlainFuture<byte[]> readData(GridGgfsHadoopStreamDelegate delegate, long pos, int len,
        @Nullable final byte[] outBuf, final int outOff, final int outLen);

    /**
     * Writes data to the stream with given streamId. This method does not return any future since
     * no response to write request is sent.
     *
     * @param delegate Stream delegate.
     * @param data Data to write.
     * @param off Offset.
     * @param len Length.
     * @throws IOException If failed.
     */
    public void writeData(GridGgfsHadoopStreamDelegate delegate, byte[] data, int off, int len) throws IOException;

    /**
     * Close server stream.
     *
     * @param delegate Stream delegate.
     * @throws IOException If failed.
     */
    public void closeStream(GridGgfsHadoopStreamDelegate delegate) throws IOException;

    /**
     * Flush output stream.
     *
     * @param delegate Stream delegate.
     * @throws IOException If failed.
     */
    public void flush(GridGgfsHadoopStreamDelegate delegate) throws IOException;
}
