/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
     * @return Number of read bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    public int read(ByteBuffer destBuf) throws IOException;

    /**
     * Reads a sequence of bytes from this file into the {@code destinationBuffer}
     * starting from specified file {@code position}.
     *
     * @param destBuf Destination byte buffer.
     * @param position Starting position of file.
     *
     * @return Number of read bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    public int read(ByteBuffer destBuf, long position) throws IOException;

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
     * Writes {@code length} bytes from the {@code buffer}
     * starting at offset {@code off} to this file.
     *
     * @param buf Source byte array.
     * @param off Start offset in the {@code buffer}.
     * @param len Number of bytes to write.
     *
     * @throws IOException If some I/O error occurs.
     */
    public void write(byte[] buf, int off, int len) throws IOException;

    public MappedByteBuffer map(int maxWalSegmentSize) throws IOException;

    /**
     * Forces any updates of this file to be written to the storage
     * device that contains it.
     *
     * @throws IOException If some I/O error occurs.
     */
    public void force() throws IOException;

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
