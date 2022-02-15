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
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.BasicRateLimiter;

public class LimitedRateFileIO extends FileIODecorator {
    private final BasicRateLimiter limiter;
    private final int blockSize;

    /**
     * @param delegate File I/O delegate
     */
    public LimitedRateFileIO(FileIO delegate, BasicRateLimiter limiter, int blockSize) {
        super(delegate);

        this.limiter = limiter;
        this.blockSize = blockSize;
    }

    /** {@inheritDoc} */
    @Override public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        long totalWritten = 0;

        try {
            while (totalWritten < count) {
                long transferCnt = Math.min(count - totalWritten, blockSize);

                limiter.acquire((int)transferCnt);

                long written = 0;

                do {
                    long pos = position + totalWritten + written;
                    long len = transferCnt - written;

                    written += super.transferTo(pos, len, target);
                }
                while (written < transferCnt);

                totalWritten += written;
            }
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteInterruptedException((InterruptedException)e.getCause());
        }

        return totalWritten;
    }

    /** {@inheritDoc} */
    @Override public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        long totalWritten = 0;

        try {
            while (totalWritten < count) {
                long transferCnt = Math.min(count - totalWritten, blockSize);

                limiter.acquire((int)transferCnt);

                long written = 0;

                do {
                    long pos = position + totalWritten + written;
                    long len = transferCnt - written;

                    written += super.transferFrom(src, pos, len);
                }
                while (written < transferCnt);

                totalWritten += written;
            }
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteInterruptedException((InterruptedException)e.getCause());
        }

        return totalWritten;
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
    @Override public int write(ByteBuffer srcBuf) throws IOException {
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
}
