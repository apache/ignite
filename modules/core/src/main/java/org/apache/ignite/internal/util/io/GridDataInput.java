/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util.io;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

/**
 * Extended data input.
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