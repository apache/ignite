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

/**
 * Decorator class for File I/O
 */
public class FileIODecorator implements FileIO {
    /** File I/O delegate */
    private final FileIO delegate;

    /**
     *
     * @param delegate File I/O delegate
     */
    public FileIODecorator(FileIO delegate) {
        this.delegate = delegate;
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
    @Override public int read(ByteBuffer destinationBuffer) throws IOException {
        return delegate.read(destinationBuffer);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destinationBuffer, long position) throws IOException {
        return delegate.read(destinationBuffer, position);
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buffer, int offset, int length) throws IOException {
        return delegate.read(buffer, offset, length);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer sourceBuffer) throws IOException {
        return delegate.write(sourceBuffer);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer sourceBuffer, long position) throws IOException {
        return delegate.write(sourceBuffer, position);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] buffer, int offset, int length) throws IOException {
        delegate.write(buffer, offset, length);
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        delegate.force();
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
