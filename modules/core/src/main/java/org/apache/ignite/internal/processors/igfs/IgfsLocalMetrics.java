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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jsr166.LongAdder8;

/**
 * Value object holding all local IGFS metrics which cannot be determined using file system traversal.
 */
public class IgfsLocalMetrics {
    /** Block reads. First value - total reads, second value - reads delegated to the secondary file system. */
    private volatile IgniteBiTuple<LongAdder8, LongAdder8> blocksRead;

    /** Block writes. First value - total writes, second value - writes delegated to the secondary file system. */
    private volatile IgniteBiTuple<LongAdder8, LongAdder8> blocksWritten;

    /** Byte reads. First value - total bytes read, second value - consumed time. */
    private volatile IgniteBiTuple<LongAdder8, LongAdder8> bytesRead;

    /** Byte writes. First value - total bytes written, second value - consumed time. */
    private volatile IgniteBiTuple<LongAdder8, LongAdder8> bytesWritten;

    /** Number of files opened for read. */
    private final LongAdder8 filesOpenedForRead = new LongAdder8();

    /** Number of files opened for write. */
    private final LongAdder8 filesOpenedForWrite = new LongAdder8();

    /**
     * Constructor.
     */
    IgfsLocalMetrics() {
        reset();
    }

    /**
     * @return Read bytes.
     */
    long readBytes() {
        return bytesRead.get1().longValue();
    }

    /**
     * @return Read bytes time.
     */
    long readBytesTime() {
        return bytesRead.get2().longValue();
    }

    /**
     * Adds given numbers to read bytes and read time.
     *
     * @param readBytes Number of bytes read.
     * @param readTime Read time.
     */
    void addReadBytesTime(long readBytes, long readTime) {
        IgniteBiTuple<LongAdder8, LongAdder8> bytesRead0 = bytesRead;

        bytesRead0.get1().add(readBytes);
        bytesRead0.get2().add(readTime);
    }

    /**
     * @return Written bytes.
     */
    long writeBytes() {
        return bytesWritten.get1().longValue();
    }

    /**
     * @return Write bytes time.
     */
    long writeBytesTime() {
        return bytesWritten.get2().longValue();
    }

    /**
     * Adds given numbers to written bytes and write time.
     *
     * @param writtenBytes Number of bytes written.
     * @param writeTime Write time.
     */
    void addWrittenBytesTime(long writtenBytes, long writeTime) {
        IgniteBiTuple<LongAdder8, LongAdder8> bytesWritten0 = bytesWritten;

        bytesWritten0.get1().add(writtenBytes);
        bytesWritten0.get2().add(writeTime);
    }

    /**
     * @return Read blocks.
     */
    long readBlocks() {
        return blocksRead.get1().longValue();
    }

    /**
     * @return Written blocks to secondary file system.
     */
    long readBlocksSecondary() {
        return blocksRead.get2().longValue();
    }

    /**
     * Adds given numbers to read blocks counters.
     *
     * @param total Total number of blocks read.
     * @param secondary Number of blocks read form secondary FS.
     */
    void addReadBlocks(int total, int secondary) {
        IgniteBiTuple<LongAdder8, LongAdder8> blocksRead0 = blocksRead;

        blocksRead0.get1().add(total);
        blocksRead0.get2().add(secondary);
    }

    /**
     * @return Written blocks.
     */
    long writeBlocks() {
        return blocksWritten.get1().longValue();
    }

    /**
     * @return Written blocks to secondary file system.
     */
    long writeBlocksSecondary() {
        return blocksWritten.get2().longValue();
    }

    /**
     * Adds given numbers to write blocks counters.
     *
     * @param total Total number of block written.
     * @param secondary Number of blocks written to secondary FS.
     */
    void addWriteBlocks(int total, int secondary) {
        IgniteBiTuple<LongAdder8, LongAdder8> blocksWritten0 = blocksWritten;

        blocksWritten0.get1().add(total);
        blocksWritten0.get2().add(secondary);
    }

    /**
     * Increment files opened for read.
     */
    void incrementFilesOpenedForRead() {
        filesOpenedForRead.increment();
    }

    /**
     * Decrement files opened for read.
     */
    void decrementFilesOpenedForRead() {
        filesOpenedForRead.decrement();
    }

    /**
     * @return Files opened for read.
     */
    int filesOpenedForRead() {
        return filesOpenedForRead.intValue();
    }

    /**
     * Increment files opened for write.
     */
    void incrementFilesOpenedForWrite() {
        filesOpenedForWrite.increment();
    }

    /**
     * Decrement files opened for write.
     */
    void decrementFilesOpenedForWrite() {
        filesOpenedForWrite.decrement();
    }

    /**
     * @return Files opened for write.
     */
    int filesOpenedForWrite() {
        return filesOpenedForWrite.intValue();
    }

    /**
     * Reset summary  counters.
     */
    void reset() {
        blocksRead = F.t(new LongAdder8(), new LongAdder8());
        blocksWritten = F.t(new LongAdder8(), new LongAdder8());
        bytesRead = F.t(new LongAdder8(), new LongAdder8());
        bytesWritten = F.t(new LongAdder8(), new LongAdder8());
    }
}