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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * The class implements a {@link FileIO} that allows to write bytes to the underlying {@link FileIO} without necessarily
 * causing a call to the underlying system for each byte written.
 * It doesn't support reading or random access. It is not designed for writing concurrently from several threads.
 */
public class BufferedFileIO extends FileIODecorator {
    /** */
    private final ByteBuffer buf;

    /** */
    private long pos;

    /** */
    public BufferedFileIO(FileIO fileIO, int bufSz) throws IOException {
        super(fileIO);

        A.ensure(fileIO != null, "fileIO must not be null");
        A.ensure(bufSz > 0, "bufSz must be positive");

        buf = ByteBuffer.allocateDirect(bufSz);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        if (buf.limit() == 0)
            throw new IOException("FileIO closed");

        int bytesCnt = srcBuf.remaining();

        int limit = srcBuf.limit();

        while (srcBuf.hasRemaining()) {
            if (srcBuf.remaining() > buf.remaining())
                srcBuf.limit(srcBuf.position() + buf.remaining());

            buf.put(srcBuf);

            if (!buf.hasRemaining()) {
                buf.flip();

                delegate.writeFully(buf);

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

        if (buf.hasRemaining())
            delegate.writeFully(buf);

        buf.position(0).limit(0);

        delegate.close();
    }

}
