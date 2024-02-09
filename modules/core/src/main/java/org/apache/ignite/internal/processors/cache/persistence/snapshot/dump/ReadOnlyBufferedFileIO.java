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

/** */
class ReadOnlyBufferedFileIO extends FileIODecorator {
    /** */
    private static final int DEFAULT_BLOCK_SIZE = 4096;

    /** */
    private final ByteBuffer buf;

    /** */
    private boolean eofReached;

    /** */
    private long pos;

    /** */
    public ReadOnlyBufferedFileIO(FileIO fileIO) {
        super(fileIO);

        A.ensure(fileIO != null, "fileIO must not be null");

        int blockSize = getFileSystemBlockSize();

        if (blockSize <= 0)
            blockSize = DEFAULT_BLOCK_SIZE;

        buf = ByteBuffer.allocate(blockSize);

        buf.position(buf.limit());
    }

    /** {@inheritDoc} */
    @Override public int readFully(ByteBuffer destBuf) throws IOException {
        if (destBuf.remaining() == 0)
            throw new IOException("dest buffer full");

        int n = 0;

        while (destBuf.hasRemaining()) {
            if (!buf.hasRemaining()) {
                if (eofReached)
                    return -1;

                buf.clear();

                int len = delegate.readFully(buf, pos);

                if (len < buf.limit())
                    eofReached = true;
                else
                    pos += len;

                buf.flip();
            }

            if (!buf.hasRemaining())
                break;

            int len = Math.min(destBuf.remaining(), buf.remaining());

            if (len > 0) {
                buf.get(destBuf.array(), destBuf.arrayOffset() + destBuf.position(), len);

                destBuf.position(destBuf.position() + len);

                n += len;
            }
        }

        return n;
    }
}
