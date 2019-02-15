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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Interface to perform file I/O operations.
 */
public interface FileIO extends AutoCloseable {
    /**
     * Returns current file position.
     *
     * @return  Current file position,
     *          a non-negative integer counting the number of bytes
     *          from the beginning of the file to the current position.
     *
     * @throws IOException If some I/O error occurs.
     */
    public long position() throws IOException;

    /**
     * Sets new current file position.
     *
     * @param  newPosition
     *         The new position, a non-negative integer counting
     *         the number of bytes from the beginning of the file.
     *
     * @throws IOException If some I/O error occurs.
     */
    public void position(long newPosition) throws IOException;

    /**
     * Reads a sequence of bytes from this file into the {@code destinationBuffer}.
     *
     * @param destBuf Destination byte buffer.
     *
     * @return Number of read bytes, or <tt>-1</tt> if the
     *          given position is greater than or equal to the file's current size
     *
     * @throws IOException If some I/O error occurs.
     */
    public int read(ByteBuffer destBuf) throws IOException;

    /**
     * Reads a sequence of bytes from this file into the {@code destinationBuffer}.
     *
     * @param destBuf Destination byte buffer.
     *
     * @return Number of written bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    public int readFully(ByteBuffer destBuf) throws IOException;

    /**
     * Reads a sequence of bytes from this file into the {@code destinationBuffer}
     * starting from specified file {@code position}.
     *
     * @param destBuf Destination byte buffer.
     * @param position Starting position of file.
     *
     * @return Number of read bytes, possibly zero, or <tt>-1</tt> if the
     *          given position is greater than or equal to the file's current
     *          size
     *
     * @throws IOException If some I/O error occurs.
     */
    public int read(ByteBuffer destBuf, long position) throws IOException;

    /**
     * Reads a sequence of bytes from this file into the {@code destinationBuffer}
     * starting from specified file {@code position}.
     *
     * @param destBuf Destination byte buffer.
     * @param position Starting position of file.
     *
     * @return Number of written bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    public int readFully(ByteBuffer destBuf, long position) throws IOException;

    /**
     * Reads a up to {@code length} bytes from this file into the {@code buffer}.
     *
     * @param buf Destination byte array.
     * @param off The start offset in array {@code b}
     *               at which the data is written.
     * @param len Maximum number of bytes read.
     *
     * @return Number of read bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    public int read(byte[] buf, int off, int len) throws IOException;

    /**
     * Reads a up to {@code length} bytes from this file into the {@code buffer}.
     *
     * @param buf Destination byte array.
     * @param off The start offset in array {@code b}
     *               at which the data is written.
     * @param len Number of bytes read.
     *
     * @return Number of written bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    public int readFully(byte[] buf, int off, int len) throws IOException;

    /**
     * Writes a sequence of bytes to this file from the {@code sourceBuffer}.
     *
     * @param srcBuf Source buffer.
     *
     * @return Number of written bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    public int write(ByteBuffer srcBuf) throws IOException;

    /**
     * Writes a sequence of bytes to this file from the {@code sourceBuffer}.
     *
     * @param srcBuf Source buffer.
     *
     * @return Number of written bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    public int writeFully(ByteBuffer srcBuf) throws IOException;

    /**
     * Writes a sequence of bytes to this file from the {@code sourceBuffer}
     * starting from specified file {@code position}
     *
     * @param srcBuf Source buffer.
     * @param position Starting file position.
     *
     * @return Number of written bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    public int write(ByteBuffer srcBuf, long position) throws IOException;

    /**
     * Writes a sequence of bytes to this file from the {@code sourceBuffer}
     * starting from specified file {@code position}
     *
     * @param srcBuf Source buffer.
     * @param position Starting file position.
     *
     * @return Number of written bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    public int writeFully(ByteBuffer srcBuf, long position) throws IOException;

    /**
     * Writes {@code length} bytes from the {@code buffer}
     * starting at offset {@code off} to this file.
     *
     * @param buf Source byte array.
     * @param off Start offset in the {@code buffer}.
     * @param len Number of bytes to write.
     *
     * @return Number of written bytes.
     * 
     * @throws IOException If some I/O error occurs.
     */
    public int write(byte[] buf, int off, int len) throws IOException;

    /**
     * Writes {@code length} bytes from the {@code buffer}
     * starting at offset {@code off} to this file.
     *
     * @param buf Source byte array.
     * @param off Start offset in the {@code buffer}.
     * @param len Number of bytes to write.
     *
     * @return Number of written bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    public int writeFully(byte[] buf, int off, int len) throws IOException;

    /**
     * Allocates memory mapped buffer for this file with given size.
     *
     * @param sizeBytes Size of buffer.
     *
     * @return Instance of mapped byte buffer.
     *
     * @throws IOException If some I/O error occurs.
     */
    public MappedByteBuffer map(int sizeBytes) throws IOException;

    /**
     * Forces any updates of this file to be written to the storage
     * device that contains it.
     *
     * @throws IOException If some I/O error occurs.
     */
    public void force() throws IOException;

    /**
     * Forces any updates of this file to be written to the storage
     * device that contains it.
     *
     * @param withMetadata If {@code true} force also file metadata.
     * @throws IOException If some I/O error occurs.
     */
    public void force(boolean withMetadata) throws IOException;

    /**
     * Returns current file size in bytes.
     *
     * @return File size.
     *
     * @throws IOException If some I/O error occurs.
     */
    public long size() throws IOException;

    /**
     * Truncates current file to zero length
     * and resets current file position to zero.
     *
     * @throws IOException If some I/O error occurs.
     */
    public void clear() throws IOException;

    /**
     * Closes current file.
     *
     * @throws IOException If some I/O error occurs.
     */
    @Override public void close() throws IOException;
}
