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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * File I/O implementation based on {@code java.io.RandomAccessFile}.
 */
public class RandomAccessFileIO implements FileIO {

    /**
     * Random access file associated with this I/O
     */
    private final RandomAccessFile file;

    /**
     * File channel associated with {@code file}
     */
    private final FileChannel channel;

    /**
     * Creates I/O implementation for specified {@code file}
     *
     * @param file Random access file
     */
    public RandomAccessFileIO(RandomAccessFile file) {
        this.file = file;
        this.channel = file.getChannel();
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        return channel.position();
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        channel.position(newPosition);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destinationBuffer) throws IOException {
        return channel.read(destinationBuffer);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destinationBuffer, long position) throws IOException {
        return channel.read(destinationBuffer, position);
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buffer, int offset, int length) throws IOException {
        return file.read(buffer, offset, length);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer sourceBuffer) throws IOException {
        return channel.write(sourceBuffer);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer sourceBuffer, long position) throws IOException {
        return channel.write(sourceBuffer, position);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] buffer, int offset, int length) throws IOException {
        file.write(buffer, offset, length);
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        channel.force(false);
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        return channel.size();
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IOException {
        channel.position(0);
        file.setLength(0);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        file.close();
    }
}
