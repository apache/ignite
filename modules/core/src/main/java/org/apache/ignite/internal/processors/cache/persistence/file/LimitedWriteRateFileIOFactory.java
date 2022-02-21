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

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import org.apache.ignite.internal.util.BasicRateLimiter;

/**
 * File I/O factory which provides {@link LimitedWriteRateFileIO} implementation of {@link FileIO}.
 */
public class LimitedWriteRateFileIOFactory implements FileIOFactory {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Default transfer block size. */
    private static final int DEFAULT_BLOCK_SIZE = 64 * 1024;

    /** Transfer rate limiter. */
    private final BasicRateLimiter rateLimiter;

    /**
     * Size of the data block to be transferred at once (in bytes).
     * <p>
     * The smaller it is, the more uniform bandwidth limitation will be obtained, however at the expense of performance.
     */
    private final int blockSize;

    /** File I/O factory delegate. */
    private final FileIOFactory delegate;

    /**
     * @param delegate File I/O factory delegate.
     */
    public LimitedWriteRateFileIOFactory(FileIOFactory delegate) {
        this(delegate, 0, DEFAULT_BLOCK_SIZE);
    }

    /**
     * @param delegate File I/O factory delegate.
     * @param rate Max transfer rate in bytes/sec.
     * @param blockSize Size of the data block to be transferred at once (in bytes).
     */
    public LimitedWriteRateFileIOFactory(FileIOFactory delegate, int rate, int blockSize) {
        this.delegate = delegate;
        this.blockSize = blockSize;

        rateLimiter = new BasicRateLimiter(rate);
    }

    /** {@inheritDoc} */
    @Override public FileIO create(File file, OpenOption... modes) throws IOException {
        FileIO fileIO = delegate.create(file, modes);

        return rateLimiter.isUnlimited() ? fileIO : new LimitedWriteRateFileIO(fileIO, rateLimiter, blockSize);
    }

    /**
     * @param rate Max transfer rate in bytes/sec.
     */
    public void setRate(int rate) {
        rateLimiter.setRate(rate);
    }
}
