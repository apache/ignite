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

package org.apache.ignite.internal.processors.cache.persistence.wal.filehandle;

import java.io.IOException;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;

/**
 * Factory of {@link FileWriteHandle}.
 */
public interface FileWriteHandleFactory {
    /**
     * @param cctx              Context.
     * @param fileIO            I/O file interface to use
     * @param rbuf
     * @param serializer        Serializer.
     * @param metrics           Data storage metrics.
     * @param writer            WAL writer.
     * @param pos               Initial position.
     * @param mode              WAL mode.
     * @param mmap              Mmap.
     * @param resume            Created on resume logging flag.
     * @param fsyncDelay        Fsync delay.
     * @param maxWalSegmentSize Max WAL segment size.
     * @return FileWriteHandle
     * @throws IOException If failed.
     */
    FileWriteHandle build(
        GridCacheSharedContext cctx, SegmentIO fileIO, SegmentedRingByteBuffer rbuf, RecordSerializer serializer,
        DataStorageMetricsImpl metrics, FileHandleManagerImpl.WALWriter writer, long pos, WALMode mode, boolean mmap,
        boolean resume, long fsyncDelay, long maxWalSegmentSize) throws IOException;
}
