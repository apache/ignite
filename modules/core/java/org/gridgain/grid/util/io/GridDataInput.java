// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.io;

import java.io.*;

/**
 * Extended data input.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridDataInput extends DataInput {
    /**
     * @param bytes Bytes.
     * @param len Length.
     */
    public void bytes(byte[] bytes, int len);

    /**
     * @param in Underlying input stream.
     * @throws IOException In case of error.
     */
    public void inputStream(InputStream in) throws IOException;

    /**
     * Resets data output.
     *
     * @throws IOException In case of error.
     */
    public void reset() throws IOException;

    /**
     * @return The next byte of data, or {@code -1} if the end of the stream is reached.
     * @exception IOException In case of error.
     */
    public int read() throws IOException;

    /**
     * @param b Buffer into which the data is read.
     * @return Total number of bytes read into the buffer, or {@code -1} is there is no
     *     more data because the end of the stream has been reached.
     * @exception IOException In case of error.
     */
    public int read(byte b[]) throws IOException;

    /**
     * @param b Buffer into which the data is read.
     * @param off Start offset.
     * @param len Maximum number of bytes to read.
     * @return Total number of bytes read into the buffer, or {@code -1} is there is no
     *     more data because the end of the stream has been reached.
     * @exception IOException In case of error.
     */
    public int read(byte b[], int off, int len) throws IOException;

    /**
     * Reads array of {@code byte}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    public byte[] readByteArray() throws IOException;

    /**
     * Reads array of {@code short}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    public short[] readShortArray() throws IOException;

    /**
     * Reads array of {@code int}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    public int[] readIntArray() throws IOException;

    /**
     * Reads array of {@code long}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    public long[] readLongArray() throws IOException;

    /**
     * Reads array of {@code float}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    public float[] readFloatArray() throws IOException;

    /**
     * Reads array of {@code double}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    public double[] readDoubleArray() throws IOException;

    /**
     * Reads array of {@code boolean}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    public boolean[] readBooleanArray() throws IOException;

    /**
     * Reads array of {@code char}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    public char[] readCharArray() throws IOException;
}
