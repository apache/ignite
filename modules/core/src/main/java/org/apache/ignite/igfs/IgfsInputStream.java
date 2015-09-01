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

package org.apache.ignite.igfs;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@code IGFS} input stream to read data from the file system.
 * It provides several additional methods for asynchronous access.
 */
public abstract class IgfsInputStream extends InputStream {
    /**
     * Gets file length during file open.
     *
     * @return File length.
     */
    public abstract long length();

    /**
     * Seek to the specified position.
     *
     * @param pos Position to seek to.
     * @throws IOException In case of IO exception.
     */
    public abstract void seek(long pos) throws IOException;

    /**
     * Get the current position in the input stream.
     *
     * @return The current position in the input stream.
     * @throws IOException In case of IO exception.
     */
    public abstract long position() throws IOException;

    /**
     * Read bytes from the given position in the stream to the given buffer.
     * Continues to read until passed buffer becomes filled.
     *
     * @param pos Position in the input stream to seek.
     * @param buf Buffer into which data is read.
     * @throws IOException In case of IO exception.
     */
    public abstract void readFully(long pos, byte[] buf) throws IOException;

    /**
     *
     * @param pos Position in the input stream to seek.
     * @param buf Buffer into which data is read.
     * @param off Offset in the buffer from which stream data should be written.
     * @param len The number of bytes to read.
     * @throws IOException In case of IO exception.
     */
    public abstract void readFully(long pos, byte[] buf, int off, int len) throws IOException;

    /**
     *
     * @param pos Position in the input stream to seek.
     * @param buf Buffer into which data is read.
     * @param off Offset in the buffer from which stream data should be written.
     * @param len The number of bytes to read.
     * @return Total number of bytes read into the buffer, or -1 if there is no more data (EOF).
     * @throws IOException In case of IO exception.
     */
    public abstract int read(long pos, byte[] buf, int off, int len) throws IOException;
}