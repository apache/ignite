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

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
abstract class FileHandle {
    /** I/O interface for read/write operations with file */
    SegmentIO fileIO;

    /** Segment idx corresponded to fileIo*/
    final long segmentIdx;

    /**
     * @param fileIO I/O interface for read/write operations of FileHandle.
     */
    public FileHandle(@NotNull SegmentIO fileIO) {
        this.fileIO = fileIO;
        segmentIdx = fileIO.getSegmentId();
    }

    /**
     * @return Absolute WAL segment file index (incremental counter).
     */
    public long getSegmentId(){
        return segmentIdx;
    }
}
