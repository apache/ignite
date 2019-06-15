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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.DumpProcessor;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockDump;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageMetaInfoStore;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.ThreadPageLocksDumpLock;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.ThreadPageLocksDumpLock.ThreadState;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.PageLockLogSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.PageLockStackSnapshot;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.BEFORE_READ_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.BEFORE_WRITE_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker.LOCK_OP_MASK;
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
        StringBuilder sb = new StringBuilder();

        ToStringHelper proc = new ToStringHelper(sb, strucutreIdMapFunc(pageLockDump));

        pageLockDump.apply(proc);

        return sb.toString();
    }

    /** */
    private static Function<Integer, String> strucutreIdMapFunc(PageLockDump pageLockDump) {
        if (pageLockDump instanceof ThreadPageLocksDumpLock) {
            ThreadPageLocksDumpLock dump = (ThreadPageLocksDumpLock)pageLockDump;

            return dump.structureIdToStrcutureName::get;
        }
        else
            return String::valueOf;
    }

    /** */
    private static class ToStringHelper implements DumpProcessor {
        /** */
        private final Function<Integer, String> strucutreIdMapFunc;

        /** */
        private final StringBuilder sb;

        /** */
        private ToStringHelper(StringBuilder sb, Function<Integer, String> strucutreIdMapFunc) {
            this.sb = sb;
            this.strucutreIdMapFunc = strucutreIdMapFunc;
        }

        /** Helper class for track lock/unlock count. */
        class LockState {
            /** */
            int readlock;

            /** */
            int writelock;
        }

        /** */
        private String operationToString(int op) {
            switch (op) {
                case BEFORE_READ_LOCK:
                    return "Try Read lock";
                case BEFORE_WRITE_LOCK:
                    return "Try Write lock";
                case READ_LOCK:
                    return "Read lock";
                case READ_UNLOCK:
                    return "Read unlock";
                case WRITE_LOCK:
                    return "Write lock";
                case WRITE_UNLOCK:
                    return "Write unlock";
            }

            return "N/A";
        }

        /**
         * @param entry Log entry.
         * @return String line.
         */
        private String buildPageInfo(PageLockLogSnapshot.LogEntry entry) {
            int op = entry.operation;
            long pageId = entry.pageId;
            int structureId = entry.structureId;

            return operationToString(op) + " pageId=" + pageId
                + ", structureId=" + strucutreIdMapFunc.apply(structureId)
                + " [pageIdHex=" + hexLong(pageId)
                + ", partId=" + partId(pageId) + ", pageIdx=" + pageIndex(pageId)
                + ", flags=" + hexInt(flag(pageId)) + "]";
        }

        /**
         * @param holdedLocks Holded locks map.
         * @return String line.
         */
        private String lockedPagesInfo(Map<Long, LockState> holdedLocks) {
            SB sb = new SB();

            sb.a("Locked pages = [");

            boolean first = true;

            for (Map.Entry<Long, LockState> entry : holdedLocks.entrySet()) {
                Long pageId = entry.getKey();
                LockState lockState = entry.getValue();

                if (!first)
                    sb.a(",");
                else
                    first = false;

                sb.a(pageId)
                    .a("[" + hexLong(pageId) + "]")
                    .a("(r=" + lockState.readlock + "|w=" + lockState.writelock + ")");
            }

            sb.a("]");

            return sb.toString();
        }

        /** {@inheritDoc} */
        @Override public void processDump(PageLockLogSnapshot snapshot) {
            Map<Long, LockState> holdetLocks = new LinkedHashMap<>();

            SB logLocksStr = new SB();

            List<PageLockLogSnapshot.LogEntry> locklog = snapshot.locklog;
            int nextOp = snapshot.nextOp;
            long nextOpPageId = snapshot.nextOpPageId;
            int nextOpStructureId = snapshot.nextOpStructureId;

            for (PageLockLogSnapshot.LogEntry entry : locklog) {
                int op = entry.operation;
                long pageId = entry.pageId;
                int locksHolded = entry.holdedLocks;

                if (op == READ_LOCK || op == WRITE_LOCK || op == BEFORE_READ_LOCK || op == BEFORE_WRITE_LOCK) {
                    LockState state = holdetLocks.get(pageId);

                    if (state == null)
                        holdetLocks.put(pageId, state = new LockState());

                    if (op == READ_LOCK)
                        state.readlock++;

                    if (op == WRITE_LOCK)
                        state.writelock++;

                    logLocksStr.a("L=" + locksHolded + " -> " + buildPageInfo(entry) + U.nl());
                }

                if (op == READ_UNLOCK || op == WRITE_UNLOCK) {
                    LockState state = holdetLocks.get(pageId);

                    if (op == READ_UNLOCK)
                        state.readlock--;

                    if (op == WRITE_UNLOCK)
                        state.writelock--;

                    if (state.readlock == 0 && state.writelock == 0)
                        holdetLocks.remove(pageId);

                    logLocksStr.a("L=" + locksHolded + " <- " + buildPageInfo(entry) + U.nl());
                }
            }

            if (nextOpPageId != 0) {
                logLocksStr.a("-> " + operationToString(nextOp) + " nextOpPageId=" + nextOpPageId +
                    ", nextOpStructureId=" + strucutreIdMapFunc.apply(nextOpStructureId)
                    + " [pageIdHex=" + hexLong(nextOpPageId)
                    + ", partId=" + partId(nextOpPageId) + ", pageIdx=" + pageIndex(nextOpPageId)
                    + ", flags=" + hexInt(flag(nextOpPageId)) + "]" + U.nl());
            }

            sb.append(lockedPagesInfo(holdetLocks)).append(U.nl());

            sb.append("Locked pages log: ").append(snapshot.name)
                .append(" time=(").append(snapshot.time).append(", ")
                .append(DATE_FMT.format(new java.util.Date(snapshot.time)))
                .append(")")
                .append(U.nl());

            sb.append(logLocksStr).append(U.nl());
            ;
        }

        /** {@inheritDoc} */
        @Override public void processDump(PageLockStackSnapshot snapshot) {
            int headIdx = snapshot.headIdx;
            PageMetaInfoStore pageIdLocksStack = snapshot.pageIdLocksStack;
            long nextOpPageId = snapshot.nextOpPageId;

            Map<Long, LockState> holdedLocks = new LinkedHashMap<>();

            SB stackStr = new SB();

            if (nextOpPageId != 0)
                stackStr.a("\t-> " + operationToString(snapshot.nextOp) +
                    " structureId=" + strucutreIdMapFunc.apply(snapshot.nextOpStructureId) +
                    " " + pageIdToString(nextOpPageId) + U.nl());

            for (int itemIdx = headIdx - 1; itemIdx >= 0; itemIdx--) {
                long pageId = pageIdLocksStack.getPageId(itemIdx);

                if (pageId == 0 && itemIdx == 0)
                    break;

                int op;

                if (pageId == 0) {
                    stackStr.a("\t -\n");

                    continue;
                }
                else {
                    op = pageIdLocksStack.getOperation(itemIdx) & LOCK_OP_MASK;

                    int structureId = pageIdLocksStack.getStructureId(itemIdx);

                    stackStr.a("\t" + operationToString(op) +
                        " structureId=" + strucutreIdMapFunc.apply(structureId) +
                        " " + pageIdToString(pageId) + U.nl());
                }

                if (op == READ_LOCK || op == WRITE_LOCK || op == BEFORE_READ_LOCK) {
                    LockState state = holdedLocks.get(pageId);

                    if (state == null)
                        holdedLocks.put(pageId, state = new LockState());

                    if (op == READ_LOCK)
                        state.readlock++;

                    if (op == WRITE_LOCK)
                        state.writelock++;

                }
            }

            sb.append(lockedPagesInfo(holdedLocks)).append(U.nl());

            sb.append("Locked pages stack: ").append(snapshot.name)
                .append(" time=(").append(snapshot.time).append(", ")
                .append(DATE_FMT.format(new java.util.Date(snapshot.time)))
                .append(")")
                .append(U.nl());

            sb.append(stackStr).append(U.nl());
        }

        /** {@inheritDoc} */
        @Override public void processDump(ThreadPageLocksDumpLock snapshot) {
            sb.append("Page locks dump:").append(U.nl()).append(U.nl());

            List<ThreadState> threadStates = new ArrayList<>(snapshot.threadStates);

            // Sort thread dump by thread names.
            threadStates.sort(new Comparator<ThreadState>() {
                /** {@inheritDoc} */
                @Override public int compare(ThreadState thState1, ThreadState thState2) {
                    return thState1.threadName.compareTo(thState2.threadName);
                }
            });

            for (ThreadState ths : threadStates) {
                sb.append("Thread=[name=").append(ths.threadName)
                    .append(", id=").append(ths.threadId)
                    .append("], state=").append(ths.state)
                    .append(U.nl());

                PageLockDump pageLockDump0;

                if (ths.invalidContext == null)
                    pageLockDump0 = ths.pageLockDump;
                else {
                    sb.append(ths.invalidContext.msg).append(U.nl());

                    pageLockDump0 = ths.invalidContext.dump;
                }

                pageLockDump0.apply(this);

                sb.append(U.nl());
            }
        }
    }
}

