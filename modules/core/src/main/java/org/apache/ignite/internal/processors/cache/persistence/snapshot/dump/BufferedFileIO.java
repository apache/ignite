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
 * This IO writes to underlying storage whole buffer thus reduces number of system calls.
 * It doesn't support reading or random access. It is not designed for writing concurrently from several threads.
 */
public class BufferedFileIO extends FileIODecorator {
    /** */
    private static final int DEFAULT_BLOCK_SIZE = 4096;

    /** */
    private ByteBuffer buf;

    /** */
    private long position;

    /** */
    public BufferedFileIO(FileIO fileIO) {
        super(fileIO);

        A.ensure(fileIO != null, "fileIO must not be null");

        int blockSize = getFileSystemBlockSize();

        if (blockSize <= 0)
            blockSize = DEFAULT_BLOCK_SIZE;

        buf = ByteBuffer.allocateDirect(blockSize);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
        if (buf == null)
            throw new IOException("FileIO closed");

        int len = srcBuf.remaining();

        writeBytes(srcBuf.array(), srcBuf.arrayOffset() + srcBuf.position(), len);

        srcBuf.position(srcBuf.position() + len);

        return len;
    }

    /**
     * Writes byte array.
     * @throws IOException If some I/O error occurs.
     */
    protected void writeBytes(byte[] srcBuf, int off, int len) throws IOException {
        if (buf == null)
            throw new IOException("FileIO closed");

        if ((len | off) < 0 || len > srcBuf.length - off)
            throw new IndexOutOfBoundsException("Range [" + off + ", " + off + " + " + len + ") out of bounds for length " + srcBuf.length);

        int p = off;

        while (p < off + len) {
            int bytesCnt = Math.min(buf.remaining(), off + len - p);

            buf.put(srcBuf, p, bytesCnt);

            if (!buf.hasRemaining())
                flush();

            p += bytesCnt;
        }
    }

    /**
     * Writes one byte.
     * @throws IOException If some I/O error occurs.
     */
    protected void writeByte(byte b) throws IOException {
        if (buf == null)
            throw new IOException("FileIO closed");

        buf.put(b);

        if (!buf.hasRemaining())
            flush();
    }

    /** */
    private void flush() throws IOException {
        buf.flip();

        int len = delegate.writeFully(buf, position);

        if (len < 0)
            throw new IOException("Couldn't write data");

        position = position + len;

        buf.clear();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (buf == null)
            return;

        flush();

        buf = null;

        super.close();
    }
}
