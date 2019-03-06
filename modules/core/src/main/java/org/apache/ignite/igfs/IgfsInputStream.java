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