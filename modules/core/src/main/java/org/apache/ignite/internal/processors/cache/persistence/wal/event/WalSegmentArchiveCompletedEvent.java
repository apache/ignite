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

package org.apache.ignite.internal.processors.cache.persistence.wal.event;

import java.io.File;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.EventAdapter;
import org.apache.ignite.events.EventType;
import org.jetbrains.annotations.NotNull;

/**
 * Event indicates there was movement of WAL segment file to archive completed
 */
public class WalSegmentArchiveCompletedEvent extends EventAdapter {
    /** Absolute WAL segment file index. */
    private long absWalSegmentIdx;

    /** Destination archive file. This file is completed and closed archive segment */
    private final File archiveFile;

    /**
     * Creates WAL segment event
     *
     * @param node Node.
     * @param absWalSegmentIdx Absolute wal segment index.
     * @param archiveFile Archive file.
     */
    public WalSegmentArchiveCompletedEvent(
        @NotNull final ClusterNode node,
        final long absWalSegmentIdx,
        final File archiveFile) {
        super(node, "", EventType.EVT_WAL_SEGMENT_ARCHIVE_COMPLETED);
        this.absWalSegmentIdx = absWalSegmentIdx;
        this.archiveFile = archiveFile;
    }

    /** @return {@link #archiveFile} */
    public File getArchiveFile() {
        return archiveFile;
    }

    /** @return {@link #absWalSegmentIdx} */
    public long getAbsWalSegmentIdx() {
        return absWalSegmentIdx;
    }
}
