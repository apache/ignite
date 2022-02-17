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

/**
 * File I/O providing the ability to limit the transfer rate.
 */
public class LimitedRateFileIO extends FileIODecorator {
    /** Transfer rate limiter. */
    private final BasicRateLimiter limiter;

    /** Size of the data block to be transferred at once (in bytes). */
    private final int blockSize;

    /**
     * @param delegate File I/O delegate
     * @param limiter Transfer rate limiter.
     * @param blockSize Size of the data block to be transferred at once (in bytes).
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
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        int len = srcBuf.remaining();

        acquire(len);

        int remain = len;

        while (remain > 0)
            remain -= super.write(srcBuf);

        return len;
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        int len = srcBuf.remaining();

        acquire(len);

        int written = 0;

        while (written < len)
            written += super.write(srcBuf, position + written);

        return written;
    }

    /** {@inheritDoc} */
    @Override public int write(byte[] buf, int off, int len) throws IOException {
        acquire(len);

        int written = 0;

        while (written < len)
            written += super.write(buf, off + written, len - written);

        return len;
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf) throws IOException {
        return super.read(destBuf);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf, long position) throws IOException {
        return super.read(destBuf, position);
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int len) throws IOException {
        return super.read(buf, off, len);
    }

    /**
     * @param permits The number of permits to acquire.
     */
    private void acquire(int permits) {
        try {
            limiter.acquire(permits);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteInterruptedException((InterruptedException)e.getCause());
        }
    }
}
