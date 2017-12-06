/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import net.smacke.jaydio.align.DirectIoByteChannelAligner;

public class DirectRandomAccessFileIO implements FileIO {

    private final DirectIoByteChannelAligner channel;

    public DirectRandomAccessFileIO(File file, OpenOption[] openOptions) throws IOException {
        boolean readOnly = false;

        for (OpenOption option : openOptions) {
            if (option == StandardOpenOption.WRITE ||
                option == StandardOpenOption.APPEND)
                readOnly = true;
        }

        channel = DirectIoByteChannelAligner.open(file, readOnly);
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        return channel.position();
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        channel.position(newPosition);
    }

    @Override public int read(ByteBuffer destinationBuffer) throws IOException {
        return channel.read(destinationBuffer);
    }

    @Override public int read(ByteBuffer destinationBuffer, long position) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
        //return channel.readBytes(destinationBuffer.array(), position);
    }

    @Override public int read(byte[] buffer, int offset, int length) throws IOException {
        return channel.readBytes( buffer, offset, length);
    }

    @Override public int write(ByteBuffer sourceBuffer) throws IOException {
        return channel.write(sourceBuffer);
    }

    @Override public int write(ByteBuffer sourceBuffer, long position) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
        // return channel.write(sourceBuffer, position);
    }

    @Override public void write(byte[] buffer, int offset, int length) throws IOException {
        channel.write(ByteBuffer.wrap(buffer, offset, length));
    }

    @Override public void force() throws IOException {
        channel.flush();
    }

    @Override public long size() throws IOException {
        return channel.size();
    }

    @Override public void clear() throws IOException {
        channel.truncate(0);
    }

    @Override public void close() throws IOException {
        channel.close();
    }
}
