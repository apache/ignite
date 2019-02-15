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

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import org.apache.ignite.internal.processors.compress.FileSystemUtils;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * File I/O implementation based on {@link FileChannel}.
 */
public class RandomAccessFileIO extends AbstractFileIO {
    /**
     * File channel.
     */
    private final FileChannel ch;

    /** Native file descriptor. */
    private final int fd;

    /** */
    private final int fsBlockSize;

    /**
     * Creates I/O implementation for specified {@code file}
     *
     * @param file File.
     * @param modes Open modes.
     */
    public RandomAccessFileIO(File file, OpenOption... modes) throws IOException {
        ch = FileChannel.open(file.toPath(), modes);
        fd = getNativeFileDescriptor(ch);
        fsBlockSize = FileSystemUtils.getFileSystemBlockSize(fd);
    }

    /**
     * @param ch File channel.
     * @return Native file descriptor.
     */
    private static int getNativeFileDescriptor(FileChannel ch) {
        FileDescriptor fd = U.field(ch, "fd");
        return U.field(fd, "fd");
    }

    /** {@inheritDoc} */
    @Override public int getFileSystemBlockSize() {
        return fsBlockSize;
    }

    /** {@inheritDoc} */
    @Override public long getSparseSize() {
        return FileSystemUtils.getSparseFileSize(fd);
    }

    /** {@inheritDoc} */
    @Override public int punchHole(long position, int len) {
        return (int)FileSystemUtils.punchHole(fd, position, len, fsBlockSize);
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        return ch.position();
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        ch.position(newPosition);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf) throws IOException {
        return ch.read(destBuf);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf, long position) throws IOException {
        return ch.read(destBuf, position);
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int len) throws IOException {
        return ch.read(ByteBuffer.wrap(buf, off, len));
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        return ch.write(srcBuf);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        return ch.write(srcBuf, position);
    }

    /** {@inheritDoc} */
    @Override public int write(byte[] buf, int off, int len) throws IOException {
        return ch.write(ByteBuffer.wrap(buf, off, len));
    }

    /** {@inheritDoc} */
    @Override public void force(boolean withMetadata) throws IOException {
        ch.force(withMetadata);
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        return ch.size();
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IOException {
        ch.truncate(0);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        ch.close();
    }

    /** {@inheritDoc} */
    @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
        return ch.map(FileChannel.MapMode.READ_WRITE, 0, sizeBytes);
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        force(false);
    }
}
