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
 * File I/O providing the ability to limit the write rate.
 * <p>
 * Note: read operations are not limited, only write and transfer operations are limited.
 */
public class LimitedWriteRateFileIO extends FileIODecorator {
    /** Transfer rate limiter. */
    private final BasicRateLimiter limiter;

    /** Size of the data block to be transferred at once (in bytes). */
    private final int blockSize;

    /**
     * @param delegate File I/O delegate
     * @param limiter Transfer rate limiter.
     * @param blockSize Size of the data block to be transferred at once (in bytes).
     */
    public LimitedWriteRateFileIO(FileIO delegate, BasicRateLimiter limiter, int blockSize) {
        super(delegate);

        this.limiter = limiter;
        this.blockSize = blockSize;
    }

    /** {@inheritDoc} */
    @Override public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return limitedTransfer((offset, len) -> super.transferTo(position + offset, len, target), count);
    }

    /** {@inheritDoc} */
    @Override public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        return limitedTransfer((offset, len) -> super.transferFrom(src, position + offset, len), count);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        return writeFully(srcBuf);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
        int count = srcBuf.remaining();

        return limitedWrite((offset, len) -> {
            srcBuf.limit(srcBuf.position() + len);

            return super.write(srcBuf);
        }, count);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        return writeFully(srcBuf, position);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
        int count = srcBuf.remaining();

        return limitedWrite((offset, len) -> {
            srcBuf.limit(srcBuf.position() + len);

            return super.write(srcBuf, position + offset);
        }, count);
    }

    /** {@inheritDoc} */
    @Override public int write(byte[] buf, int off, int count) throws IOException {
        return writeFully(buf, off, count);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(byte[] buf, int off, int count) throws IOException {
        return limitedWrite((offset, len) -> super.write(buf, off + offset, len), count);
    }

    /**
     * @param writeOp Write operation.
     * @param count Number of bytes to write.
     * @return Number of written bytes.
     * @throws IOException if failed.
     */
    private int limitedWrite(WriteOperation writeOp, int count) throws IOException {
        int pos = 0;

        while (pos < count) {
            int blockLen = Math.min(count - pos, blockSize);

            acquire(blockLen);

            int written = 0;

            do {
                written += writeOp.run(pos + written, blockLen - written);
            }
            while (written < blockLen);

            pos += written;
        }

        return count;
    }

    /**
     * @param transferOp Transfer operation.
     * @param count Number of bytes to transfer.
     * @return Number of transferred bytes.
     * @throws IOException if failed.
     */
    private long limitedTransfer(TransferOperation transferOp, long count) throws IOException {
        long pos = 0;

        while (pos < count) {
            long blockLen = Math.min(count - pos, blockSize);

            acquire((int)blockLen);

            long written = 0;

            do {
                written += transferOp.run(pos + written, blockLen - written);
            }
            while (written < blockLen);

            pos += written;
        }

        return pos;
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

    /** Transfer operation. */
    private interface TransferOperation {
        /**
         * @param offset Operation offset.
         * @param count Maximum number of bytes to be transferred.
         *
         * @return Number of bytes operated.
         */
        public long run(long offset, long count) throws IOException;
    }

    /** Write operation. */
    private interface WriteOperation {
        /**
         * @param offset Operation offset.
         * @param count Maximum number of bytes to be transferred.
         *
         * @return Number of bytes operated.
         */
        public int run(int offset, int count) throws IOException;
    }
}
