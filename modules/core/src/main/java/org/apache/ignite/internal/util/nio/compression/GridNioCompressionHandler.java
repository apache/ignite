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

package org.apache.ignite.internal.util.nio.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Thread safe compression handler.
 */
public final class GridNioCompressionHandler extends BlockingCompressionHandler {
    /** Compress lock. */
    private final ReentrantLock compressLock = new ReentrantLock();

    /** Decompress lock. */
    private final ReentrantLock decompressLock = new ReentrantLock();

    /**
     * Creates handler.
     *
     * @param engine Compression engine instance for this handler.
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param log Logger to use.
     * @param encBuf Encoded buffer to be used.
     */
    public GridNioCompressionHandler(CompressionEngine engine, boolean directBuf, ByteOrder order,
        IgniteLogger log, @Nullable ByteBuffer encBuf) {
        super(engine, directBuf, order, log, encBuf);
    }

    /**
     * Lock handler.
     */
    public void compressLock() {
        compressLock.lock();
    }

    /**
     * Unlock handler.
     */
    public void compressUnlock() {
        compressLock.unlock();
    }

    /**
     * Lock handler.
     */
    public void decompressLock() {
        decompressLock.lock();
    }

    /**
     * Unlock handler.
     */
    public void decompressUnlock() {
        decompressLock.unlock();
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer compress(ByteBuffer buf) throws IOException {
        assert compressLock.isHeldByCurrentThread();

        return super.compress(buf);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer decompress(ByteBuffer buf) throws IOException {
        assert decompressLock.isHeldByCurrentThread();

        return super.decompress(buf);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer getOutputBuffer() {
        assert compressLock.isHeldByCurrentThread();

        return super.getOutputBuffer();
    }
}
