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
import java.nio.channels.WritableByteChannel;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.BasicRateLimiter;

public class LimitedFileIO extends FileIODecorator {
    private final BasicRateLimiter limiter;
    private final int blockSize;

    /**
     * @param delegate File I/O delegate
     */
    public LimitedFileIO(FileIO delegate, BasicRateLimiter limiter, int blockSize) {
        super(delegate);

        this.limiter = limiter;
        this.blockSize = blockSize;
    }

    @Override public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        if (count > Integer.MAX_VALUE)
            throw new UnsupportedOperationException("");

        int totalWritten = 0;

        try {
            while (totalWritten < count) {
                int transferCnt = Math.min((int)(count - totalWritten), blockSize);

                limiter.acquire(transferCnt);

                int written = 0;

                do {
                    long pos = position + totalWritten + written;
                    long len = transferCnt - written;

                    written += (int)super.transferTo(pos, len, target);
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
}
