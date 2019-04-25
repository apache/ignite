/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.events;

import java.io.File;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.NotNull;

/**
 * Event indicates the completion of WAL segment compaction.
 * <p>
 * {@link #getArchiveFile()} corresponds to compacted file.
 */
public class WalSegmentCompactedEvent extends WalSegmentArchivedEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates WAL segment compaction event.
     *
     * @param node Node.
     * @param absWalSegmentIdx Absolute wal segment index.
     * @param archiveFile Compacted archive file.
     */
    public WalSegmentCompactedEvent(
        @NotNull final ClusterNode node,
        final long absWalSegmentIdx,
        final File archiveFile) {
        super(node, absWalSegmentIdx, archiveFile, EventType.EVT_WAL_SEGMENT_COMPACTED);
    }
}
