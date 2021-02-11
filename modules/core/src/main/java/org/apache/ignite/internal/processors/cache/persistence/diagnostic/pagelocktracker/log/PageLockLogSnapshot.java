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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.DumpProcessor;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockDump;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageMetaInfoStore;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.LOCK_IDX_MASK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.LOCK_OP_MASK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.OP_OFFSET;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpProcessor.toStringDump;

/**
 * Page lock log snapshot.
 */
public class PageLockLogSnapshot implements PageLockDump {
    /** Page lock log name. */
    public final String name;

    /** Creation time. */
    public final long time;

    /** Head position. */
    public final int headIdx;

    /** List of log entries. */
    public final List<LogEntry> locklog;

    /** */
    public final PageMetaInfoStore log;

    /** Next operation. */
    public final int nextOp;

    /** Next data structure. */
    public final int nextOpStructureId;

    /** Next page id. */
    public final long nextOpPageId;

    /**
     *
     */
    public PageLockLogSnapshot(
        String name,
        long time,
        int headIdx,
        PageMetaInfoStore log,
        int nextOp,
        int nextOpStructureId,
        long nextOpPageId
    ) {
        this.name = name;
        this.time = time;
        this.headIdx = headIdx;
        this.log = log;
        this.locklog = toList(log);
        this.nextOp = nextOp;
        this.nextOpStructureId = nextOpStructureId;
        this.nextOpPageId = nextOpPageId;
    }

    /**
     * Convert log to list {@link PageLockLogSnapshot.LogEntry}.
     *
     * @return List of {@link PageLockLogSnapshot.LogEntry}.
     */
    private List<PageLockLogSnapshot.LogEntry> toList(PageMetaInfoStore pages) {
        List<PageLockLogSnapshot.LogEntry> lockLog = new ArrayList<>(pages.capacity());

        for (int itemIdx = 0; itemIdx < headIdx; itemIdx++) {
            int metaOnLock = pages.getOperation(itemIdx);

            assert metaOnLock != 0;

            int heldLocks = (int)(metaOnLock & LOCK_IDX_MASK) >> OP_OFFSET;

            assert heldLocks >= 0;

            int op = metaOnLock & LOCK_OP_MASK;

            int structureId = pages.getStructureId(itemIdx);

            long pageId = pages.getPageId(itemIdx);

            lockLog.add(new PageLockLogSnapshot.LogEntry(pageId, structureId, op, heldLocks));
        }

        return lockLog;
    }

    /**
     * Log entry.
     */
    public static class LogEntry {
        /** */
        public final long pageId;

        /** */
        public final int structureId;

        /** */
        public final int operation;

        /** */
        public final int holdedLocks;

        /** */
        public LogEntry(long pageId, int structureId, int operation, int holdedLock) {
            this.pageId = pageId;
            this.structureId = structureId;
            this.operation = operation;
            this.holdedLocks = holdedLock;
        }
    }

    /** {@inheritDoc} */
    @Override public void apply(DumpProcessor dumpProcessor) {
        dumpProcessor.processDump(this);
    }

    /** {@inheritDoc} */
    @Override public long time() {
        return time;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toStringDump(this);
    }
}
