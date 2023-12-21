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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import org.apache.ignite.internal.processors.cache.persistence.file.AbstractFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * {@link FileIO} that allows to write file with buffering.
 * It doesn't support reading or random access.
 * It is not designed for writing concurrently from several threads.
 */
public class WriteOnlyBufferedFileIO extends AbstractFileIO {
    /** */
    private final WritableByteChannel ch;

    /** */
    private final ByteBuffer buf;

    /** */
    private long pos;

    /** */
    public WriteOnlyBufferedFileIO(File file, int bufSize, OpenOption... modes) throws IOException {
        buf = ByteBuffer.allocateDirect(bufSize);

        ch = FileChannel.open(file.toPath(), modes);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        if (buf.limit() == 0)
            throw new IOException("FileIO closed");

        int bytesCnt = srcBuf.remaining();

        while (srcBuf.hasRemaining()) {
            int limit = srcBuf.limit();

            if (srcBuf.remaining() > buf.remaining())
                srcBuf.limit(srcBuf.position() + buf.remaining());

            buf.put(srcBuf);

            if (!buf.hasRemaining()) {
                buf.flip();

                while (buf.hasRemaining())
                    ch.write(buf);

                buf.limit(buf.capacity()).position(0);
            }

            srcBuf.limit(limit);
        }

        pos += bytesCnt;

        return bytesCnt;
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        return pos;
    }


    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        buf.flip();

        while (buf.hasRemaining())
            ch.write(buf);

        buf.position(0).limit(0);

        ch.close();
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf, long position) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int write(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void force(boolean withMetadata) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        force(false);
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getFileSystemBlockSize() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public int punchHole(long position, int len) {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public long getSparseSize() {
        return -1;
    }
}
