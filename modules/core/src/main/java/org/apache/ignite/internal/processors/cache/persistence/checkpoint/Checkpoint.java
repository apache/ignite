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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.GridConcurrentMultiPairQueue;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Data class of checkpoint information.
 */
class Checkpoint {
    /** Checkpoint entry. */
    @Nullable final CheckpointEntry cpEntry;

    /** Checkpoint pages. */
    final GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> cpPages;

    /** Checkpoint progress status. */
    final CheckpointProgressImpl progress;

    /** WAL segments fully covered by this checkpoint. */
    IgniteBiTuple<Long, Long> walSegsCoveredRange;

    /** Number of dirty pages. */
    final int pagesSize;

    /**
     * Constructor.
     *
     * @param cpEntry Checkpoint entry.
     * @param cpPages Pages to write to the page store.
     * @param progress Checkpoint progress status.
     */
    Checkpoint(
        @Nullable CheckpointEntry cpEntry,
        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> cpPages,
        CheckpointProgressImpl progress
    ) {
        this.cpEntry = cpEntry;
        this.cpPages = cpPages;
        this.progress = progress;

        pagesSize = cpPages.initialSize();
    }

    /**
     * @return {@code true} if this checkpoint contains at least one dirty page.
     */
    public boolean hasDelta() {
        return pagesSize != 0;
    }

    /**
     * @param walSegsCoveredRange WAL segments fully covered by this checkpoint.
     */
    public void walSegsCoveredRange(final IgniteBiTuple<Long, Long> walSegsCoveredRange) {
        this.walSegsCoveredRange = walSegsCoveredRange;
    }
}
