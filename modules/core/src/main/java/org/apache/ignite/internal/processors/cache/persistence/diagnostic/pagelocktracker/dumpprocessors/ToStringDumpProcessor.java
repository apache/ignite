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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors;

import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockDump;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.DumpProcessor;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.ThreadPageLocksDumpLock;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.PageLockLogSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.PageLockStackSnapshot;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.BEFORE_READ_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.BEFORE_WRITE_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.READ_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.READ_UNLOCK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.WRITE_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.WRITE_UNLOCK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.pageIdToString;
import static org.apache.ignite.internal.util.IgniteUtils.hexInt;
import static org.apache.ignite.internal.util.IgniteUtils.hexLong;

/**
 * Proccessor for buils string from {@link PageLockDump}.
 */
public class ToStringDumpProcessor {
    /** Date format. */
    public static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * @param pageLockDump Dump.
     * @return String representation of dump.
     */
    public static String toStringDump(PageLockDump pageLockDump) {
        SB sb = new SB();

        pageLockDump.apply(new DumpProcessor() {
            /** Helper class for track lock/unlock count. */
            class LockState {
                int readlock;
                int writelock;
            }

            /** {@inheritDoc} */
            @Override public void processDump(PageLockLogSnapshot snapshot) {
                String name = snapshot.name;
                List<PageLockLogSnapshot.LogEntry> locklog = snapshot.locklog;
                int nextOp = snapshot.nextOp;
                long nextOpPageId = snapshot.nextOpPageId;
                int nextOpStructureId = snapshot.nextOpStructureId;

                sb.a(name).a("\n");

                Map<Long, LockState> holdetLocks = new LinkedHashMap<>();

                SB logLocksStr = new SB();

                for (PageLockLogSnapshot.LogEntry entry : locklog) {
                    String opStr = "N/A";

                    int op = entry.operation;
                    long pageId = entry.pageId;
                    int cacheId = entry.structureId;
                    int idx = entry.holdedLocks;

                    switch (op) {
                        case READ_LOCK:
                            opStr = "Read lock    ";
                            break;
                        case READ_UNLOCK:
                            opStr = "Read unlock  ";
                            break;
                        case WRITE_LOCK:
                            opStr = "Write lock    ";
                            break;
                        case WRITE_UNLOCK:
                            opStr = "Write unlock  ";
                            break;
                    }

                    if (op == READ_LOCK || op == WRITE_LOCK || op == BEFORE_READ_LOCK) {
                        LockState state = holdetLocks.get(pageId);

                        if (state == null)
                            holdetLocks.put(pageId, state = new LockState());

                        if (op == READ_LOCK)
                            state.readlock++;

                        if (op == WRITE_LOCK)
                            state.writelock++;

                        logLocksStr.a("L=" + idx + " -> " + opStr + " nextOpPageId=" + pageId + ", nextOpCacheId=" + cacheId
                            + " [pageIdxHex=" + hexLong(pageId)
                            + ", partId=" + pageId(pageId) + ", pageIdx=" + pageIndex(pageId)
                            + ", flags=" + hexInt(flag(pageId)) + "]\n");
                    }

                    if (op == READ_UNLOCK || op == WRITE_UNLOCK) {
                        LockState state = holdetLocks.get(pageId);

                        if (op == READ_UNLOCK)
                            state.readlock--;

                        if (op == WRITE_UNLOCK)
                            state.writelock--;

                        if (state.readlock == 0 && state.writelock == 0)
                            holdetLocks.remove(pageId);

                        logLocksStr.a("L=" + idx + " <- " + opStr + " nextOpPageId=" + pageId + ", nextOpCacheId=" + cacheId
                            + " [pageIdxHex=" + hexLong(pageId)
                            + ", partId=" + pageId(pageId) + ", pageIdx=" + pageIndex(pageId)
                            + ", flags=" + hexInt(flag(pageId)) + "]\n");
                    }
                }

                if (nextOpPageId != 0) {
                    String opStr = "N/A";

                    switch (nextOp) {
                        case BEFORE_READ_LOCK:
                            opStr = "Try read lock    ";
                            break;
                        case BEFORE_WRITE_LOCK:
                            opStr = "Try write lock  ";
                            break;
                    }

                    logLocksStr.a("-> " + opStr + " nextOpPageId=" + nextOpPageId +
                        ", nextOpStructureId=" + nextOpStructureId
                        + " [pageIdxHex=" + hexLong(nextOpPageId)
                        + ", partId=" + pageId(nextOpPageId) + ", pageIdx=" + pageIndex(nextOpPageId)
                        + ", flags=" + hexInt(flag(nextOpPageId)) + "]\n");
                }

                SB holdetLocksStr = new SB();

                holdetLocksStr.a("locked pages = [");

                boolean first = true;

                for (Map.Entry<Long, LockState> entry : holdetLocks.entrySet()) {
                    Long pageId = entry.getKey();
                    LockState lockState = entry.getValue();

                    if (!first)
                        holdetLocksStr.a(",");
                    else
                        first = false;

                    holdetLocksStr.a(pageId).a("(r=" + lockState.readlock + "|w=" + lockState.writelock + ")");
                }

                holdetLocksStr.a("]\n");

                sb.a(holdetLocksStr);
                sb.a(logLocksStr);
            }

            /** {@inheritDoc} */
            @Override public void processDump(PageLockStackSnapshot snapshot) {
                String name = snapshot.name;
                long time = snapshot.time;

                int headIdx = snapshot.headIdx;
                long[] pageIdLocksStack = snapshot.pageIdLocksStack;
                long nextOpPageId = snapshot.nextOpPageId;
                int nextOp = snapshot.nextOp;

                sb.a("Locked pages stack: " + name + " time=(" +
                    time + ", " + DATE_FMT.format(new java.util.Date(time)) + ")")
                    .a("\n");

                if (nextOpPageId != 0) {
                    String str = "N/A";

                    if (nextOp == BEFORE_READ_LOCK)
                        str = "read lock";
                    else if (nextOp == BEFORE_WRITE_LOCK)
                        str = "write lock";

                    sb.a("\t-> try " + str + ", " + pageIdToString(nextOpPageId) + "\n");
                }

                for (int i = headIdx - 1; i >= 0; i--) {
                    long pageId = pageIdLocksStack[i];

                    if (pageId == 0 && i == 0)
                        break;

                    if (pageId == 0)
                        sb.a("\t" + i + " -\n");
                    else
                        sb.a("\t" + i + " " + pageIdToString(pageId) + "\n");
                }
            }

            /** {@inheritDoc} */
            @Override public void processDump(ThreadPageLocksDumpLock snapshot) {
                sb.a("Page lock dump:").a("\n").a("\n");

                for (ThreadPageLocksDumpLock.ThreadState ths : snapshot.threadStates) {
                    sb.a("Thread=[name=" + ths.threadName + ", id=" + ths.threadId + "], state=" + ths.state).a("\n");

                    PageLockDump pageLockDump0;

                    if (ths.invalidContext == null)
                        pageLockDump0 = ths.pageLockDump;
                    else {
                        sb.a(ths.invalidContext.msg).a("\n");

                        pageLockDump0 = ths.invalidContext.dump;
                    }

                    sb.a(toStringDump(pageLockDump0)).a("\n");
                }
            }
        });

        return sb.toString();
    }
}
