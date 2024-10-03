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

package org.apache.ignite.internal.jdbc2.lob;

import java.io.IOException;

/**
 * Contract for different storages providing random access to binary data.
 *
 * <p>Used by the {@link org.apache.ignite.internal.jdbc2.lob.JdbcBlobBuffer}
 */
interface JdbcBlobStorage {
    /**
     * @return Total number of bytes in the storage.
     */
    long totalCnt();

    /**
     * @return New pointer instance pointing to a zero position in the storage.
     */
    JdbcBlobBufferPointer createPointer();

    /**
     * Read a byte from this storage from specified position {@code pos}.
     *
     * @param pos Pointer to a position.
     * @return Byte read from the Blob. -1 if EOF.
     * @throws IOException if an I/O error occurs.
     */
    int read(JdbcBlobBufferPointer pos) throws IOException;

    /**
     * Read {@code cnt} bytes from this storage from specified position {@code pos}.
     *
     * @param pos Pointer to a position.
     * @param res Output byte array to write to.
     * @param off Offset in the output array to start write to.
     * @param cnt Number of bytes to read.
     * @return Number of bytes read. -1 if EOF.
     * @throws IOException if an I/O error occurs.
     */
    int read(JdbcBlobBufferPointer pos, byte[] res, int off, int cnt) throws IOException;

    /**
     * Write a byte to this storage to specified position {@code pos}.
     *
     * <p>The byte to be written is the eight low-order bits of the
     * argument {@code b}. The 24 high-order bits of {@code b}b are ignored.
     *
     * @param pos Pointer to a position.
     * @param b Byte to write.
     * @throws IOException if an I/O error occurs.
     */
    void write(JdbcBlobBufferPointer pos, int b) throws IOException;

    /**
     * Writes {@code len} bytes from the specified byte array {@code bytes} starting at offset {@code off}
     * to this storage to specified position {@code pos}.
     *
     * @param pos Pointer to a position.
     * @param bytes Input byte array.
     * @param off Start offset in the input array.
     * @param len Number of bytes to write.
     * @throws IOException if an I/O error occurs.
     */
    void write(JdbcBlobBufferPointer pos, byte[] bytes, int off, int len) throws IOException;

    /**
     * Move a position pointer {@code pos} forward by {@code step}.
     * @param pos Pointer to modify.
     * @param step Number of bytes to move forward.
     */
    void advance(JdbcBlobBufferPointer pos, long step);

    /**
     * Truncate this storage to specified length.
     *
     * @param len Length to truncate to. Must not be less than total bytes count in the storage.
     * @throws IOException if an I/O error occurs.
     */
    void truncate(long len) throws IOException;

    /**
     * Close this storage and release all resources used to access it.
     */
    void close();
}
