/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.igfs.secondary;

import java.io.Closeable;
import java.io.IOException;

/**
 * The simplest data input interface to read from secondary file system.
 */
public interface IgfsSecondaryFileSystemPositionedReadable extends Closeable {
    /**
     * Read up to the specified number of bytes, from a given position within a file, and return the number of bytes
     * read.
     *
     * @param pos Position in the input stream to seek.
     * @param buf Buffer into which data is read.
     * @param off Offset in the buffer from which stream data should be written.
     * @param len The number of bytes to read.
     * @return Total number of bytes read into the buffer, or -1 if there is no more data (EOF).
     * @throws IOException In case of any exception.
     */
    public int read(long pos, byte[] buf, int off, int len) throws IOException;
}