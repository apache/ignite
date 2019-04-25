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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Decorator class for File I/O
 */
public class FileIODecorator extends AbstractFileIO {
    /** File I/O delegate */
    protected final FileIO delegate;

    /**
     *
     * @param delegate File I/O delegate
     */
    public FileIODecorator(FileIO delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public int getFileSystemBlockSize() {
        return delegate.getFileSystemBlockSize();
    }

    /** {@inheritDoc} */
    @Override public long getSparseSize() {
        return delegate.getSparseSize();
    }

    /** {@inheritDoc} */
    @Override public int punchHole(long pos, int len) {
        return delegate.punchHole(pos, len);
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        return delegate.position();
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        delegate.position(newPosition);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf) throws IOException {
        return delegate.read(destBuf);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf, long position) throws IOException {
        return delegate.read(destBuf, position);
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int len) throws IOException {
        return delegate.read(buf, off, len);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        return delegate.write(srcBuf);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        return delegate.write(srcBuf, position);
    }

    /** {@inheritDoc} */
    @Override public int write(byte[] buf, int off, int len) throws IOException {
        return delegate.write(buf, off, len);
    }

    /** {@inheritDoc} */
    @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
        return delegate.map(sizeBytes);
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        delegate.force();
    }

    /** {@inheritDoc} */
    @Override public void force(boolean withMetadata) throws IOException {
        delegate.force(withMetadata);
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        return delegate.size();
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IOException {
        delegate.clear();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        delegate.close();
    }
}
