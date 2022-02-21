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
        return limitedTransfer((pos, len) -> super.transferTo(pos, len, target), position, count);
    }

    /** {@inheritDoc} */
    @Override public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        return limitedTransfer((pos, len) -> super.transferFrom(src, pos, len), position, count);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        int count = srcBuf.remaining();

        return limitedWrite((offs, len) -> {
            srcBuf.limit(srcBuf.position() + len);

            return super.write(srcBuf);
        }, count);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        int count = srcBuf.remaining();

        return limitedWrite((offs, len) -> {
            srcBuf.limit(srcBuf.position() + len);

            return super.write(srcBuf, position + offs);
        }, count);
    }

    /** {@inheritDoc} */
    @Override public int write(byte[] buf, int off, int count) throws IOException {
        return limitedWrite((offs, len) -> super.write(buf, off + offs, len), count);
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
     * @param writeOp Write operation.
     * @param count Number of bytes to write.
     * @return Number of written bytes.
     * @throws IOException if failed.
     */
    private int limitedWrite(WriteOperation writeOp, int count) throws IOException {
        int totalWritten = 0;

        while (totalWritten < count) {
            int transferCnt = Math.min(count - totalWritten, blockSize);

            acquire(transferCnt);

            int written = 0;

            do {
                int len = transferCnt - written;

                written += writeOp.run(totalWritten, len);
            }
            while (written < transferCnt);

            totalWritten += written;
        }

        return count;
    }

    /**
     * @param transferOp Transfer operation.
     * @param position The relative offset of the written channel.
     * @param count Number of bytes to transfer.
     * @return Number of transferred bytes.
     * @throws IOException if failed.
     */
    private long limitedTransfer(TransferOperation transferOp, long position, long count) throws IOException {
        long totalWritten = 0;

        while (totalWritten < count) {
            long transferCnt = Math.min(count - totalWritten, blockSize);

            acquire((int)transferCnt);

            long written = 0;

            do {
                long pos = position + totalWritten + written;
                long len = transferCnt - written;

                written += transferOp.run(pos, len);
            }
            while (written < transferCnt);

            totalWritten += written;
        }

        return totalWritten;
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

    /**
     *
     */
    protected interface TransferOperation {
        /**
         * @param pos The position within the file at which the transfer is to begin.
         * @param count The maximum number of bytes to be transferred.
         *
         * @return Number of bytes operated.
         */
        public long run(long pos, long count) throws IOException;
    }

    /**
     *
     */
    protected interface WriteOperation {
        /**
         * @param pos The position within the file at which the transfer is to begin.
         * @param count The maximum number of bytes to be transferred.
         *
         * @return Number of bytes operated.
         */
        public int run(int pos, int count) throws IOException;
    }
}
