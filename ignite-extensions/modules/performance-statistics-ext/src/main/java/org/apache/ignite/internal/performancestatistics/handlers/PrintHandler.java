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

package org.apache.ignite.internal.performancestatistics.handlers;

import java.io.PrintStream;
import java.util.BitSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsHandler;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.performancestatistics.util.Utils.printEscaped;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_START;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CHECKPOINT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.PAGES_WRITE_THROTTLE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TASK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_COMMIT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_ROLLBACK;

/**
 * Handler to print performance statistics operations.
 */
public class PrintHandler implements PerformanceStatisticsHandler {
    /** Print stream. */
    private final PrintStream ps;

    /** Operation types. */
    @Nullable private final BitSet ops;

    /** Start time from. */
    private final long from;

    /** Start time to. */
    private final long to;

    /** Cache identifiers to filter the output. */
    @Nullable private final Set<Integer> cacheIds;

    /**
     * @param ps Print stream.
     * @param ops Set of operations to print.
     * @param from The minimum operation start time to filter the output.
     * @param to The maximum operation start time to filter the output.
     * @param cacheIds Cache identifiers to filter the output.
     */
    public PrintHandler(PrintStream ps, @Nullable BitSet ops, long from, long to, @Nullable Set<Integer> cacheIds) {
        this.ps = ps;
        this.ops = ops;
        this.from = from;
        this.to = to;
        this.cacheIds = cacheIds;
    }

    /** {@inheritDoc} */
    @Override public void cacheStart(UUID nodeId, int cacheId, String name) {
        if (skip(CACHE_START, cacheId))
            return;

        ps.print("{\"op\":\"" + CACHE_START);
        ps.print("\",\"nodeId\":\"");
        ps.print(nodeId);
        ps.print("\",\"cacheId\":");
        ps.print(cacheId);
        ps.print(",\"name\":\"");
        printEscaped(ps, name);
        ps.println("\"}");
    }

    /** {@inheritDoc} */
    @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime, long duration) {
        if (skip(type, startTime, cacheId))
            return;

        ps.print("{\"op\":\"");
        ps.print(type);
        ps.print("\",\"nodeId\":\"");
        ps.print(nodeId);
        ps.print("\",\"cacheId\":");
        ps.print(cacheId);
        ps.print(",\"startTime\":");
        ps.print(startTime);
        ps.print(",\"duration\":");
        ps.print(duration);
        ps.println("}");
    }

    /** {@inheritDoc} */
    @Override public void transaction(UUID nodeId, GridIntList cacheIds, long startTime, long duration,
        boolean commited) {
        OperationType op = commited ? TX_COMMIT : TX_ROLLBACK;

        if (skip(op, startTime, cacheIds))
            return;

        ps.print("{\"op\":\"");
        ps.print(op);
        ps.print("\",\"nodeId\":\"");
        ps.print(nodeId);
        ps.print("\",\"cacheIds\":");
        ps.print(cacheIds);
        ps.print(",\"startTime\":");
        ps.print(startTime);
        ps.print(",\"duration\":");
        ps.print(duration);
        ps.println("}");
    }

    /** {@inheritDoc} */
    @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime,
        long duration, boolean success) {
        if (skip(QUERY, startTime))
            return;

        ps.print("{\"op\":\"" + QUERY);
        ps.print("\",\"nodeId\":\"");
        ps.print(nodeId);
        ps.print("\",\"type\":\"");
        ps.print(type);
        ps.print("\",\"text\":\"");
        printEscaped(ps, text);
        ps.print("\",\"id\":");
        ps.print(id);
        ps.print(",\"startTime\":");
        ps.print(startTime);
        ps.print(",\"duration\":");
        ps.print(duration);
        ps.print(",\"success\":");
        ps.print(success);
        ps.println("}");
    }

    /** {@inheritDoc} */
    @Override public void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
        long logicalReads, long physicalReads) {
        if (skip(QUERY_READS))
            return;

        ps.print("{\"op\":\"" + QUERY_READS);
        ps.print("\",\"nodeId\":\"");
        ps.print(nodeId);
        ps.print("\",\"type\":\"");
        ps.print(type);
        ps.print("\",\"queryNodeId\":\"");
        ps.print(queryNodeId);
        ps.print("\",\"id\":");
        ps.print(id);
        ps.print(",\"logicalReads\":");
        ps.print(logicalReads);
        ps.print(",\"physicalReads\":");
        ps.print(physicalReads);
        ps.println("}");
    }

    /** {@inheritDoc} */
    @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
        int affPartId) {
        if (skip(TASK, startTime))
            return;

        ps.print("{\"op\":\"" + TASK);
        ps.print("\",\"nodeId\":\"");
        ps.print(nodeId);
        ps.print("\",\"sesId\":\"");
        ps.print(sesId);
        ps.print("\",\"taskName\":\"");
        printEscaped(ps, taskName);
        ps.print("\",\"startTime\":");
        ps.print(startTime);
        ps.print(",\"duration\":");
        ps.print(duration);
        ps.print(",\"affPartId\":");
        ps.print(affPartId);
        ps.println("}");
    }

    /** {@inheritDoc} */
    @Override public void job(UUID nodeId, IgniteUuid sesId, long queuedTime, long startTime, long duration,
        boolean timedOut) {
        if (skip(JOB, startTime))
            return;

        ps.print("{\"op\":\"" + JOB);
        ps.print("\",\"nodeId\":\"");
        ps.print(nodeId);
        ps.print("\",\"sesId\":\"");
        ps.print(sesId);
        ps.print("\",\"queuedTime\":");
        ps.print(queuedTime);
        ps.print(",\"startTime\":");
        ps.print(startTime);
        ps.print(",\"duration\":");
        ps.print(duration);
        ps.print(",\"timedOut\":");
        ps.print(timedOut);
        ps.println("}");
    }

    /** {@inheritDoc} */
    @Override public void checkpoint(UUID nodeId, long beforeLockDuration, long lockWaitDuration, long listenersExecDuration,
        long markDuration, long lockHoldDuration, long pagesWriteDuration, long fsyncDuration,
        long walCpRecordFsyncDuration, long writeCpEntryDuration, long splitAndSortCpPagesDuration, long totalDuration,
        long cpStartTime, int pagesSize, int dataPagesWritten, int cowPagesWritten) {
        if (skip(CHECKPOINT, cpStartTime))
            return;

        ps.print("{\"op\":\"" + CHECKPOINT);
        ps.print("\",\"nodeId\":\"");
        ps.print(nodeId);
        ps.print("\",\"beforeLockDuration\":");
        ps.print(beforeLockDuration);
        ps.print(",\"lockWaitDuration\":");
        ps.print(lockWaitDuration);
        ps.print(",\"listenersExecDuration\":");
        ps.print(listenersExecDuration);
        ps.print(",\"markDuration\":");
        ps.print(markDuration);
        ps.print(",\"lockHoldDuration\":");
        ps.print(lockHoldDuration);
        ps.print(",\"pagesWriteDuration\":");
        ps.print(pagesWriteDuration);
        ps.print(",\"fsyncDuration\":");
        ps.print(fsyncDuration);
        ps.print(",\"walCpRecordFsyncDuration\":");
        ps.print(walCpRecordFsyncDuration);
        ps.print(",\"writeCpEntryDuration\":");
        ps.print(writeCpEntryDuration);
        ps.print(",\"splitAndSortCpPagesDuration\":");
        ps.print(splitAndSortCpPagesDuration);
        ps.print(",\"totalDuration\":");
        ps.print(totalDuration);
        ps.print(",\"startTime\":");
        ps.print(cpStartTime);
        ps.print(",\"pagesSize\":");
        ps.print(pagesSize);
        ps.print(",\"dataPagesWritten\":");
        ps.print(dataPagesWritten);
        ps.print(",\"cowPagesWritten\":");
        ps.print(cowPagesWritten);
        ps.println("}");
    }

    /** {@inheritDoc} */
    @Override public void pagesWriteThrottle(UUID nodeId, long endTime, long duration) {
        long startTime = endTime - duration;

        if (skip(PAGES_WRITE_THROTTLE, startTime))
            return;

        ps.print("{\"op\":\"" + PAGES_WRITE_THROTTLE);
        ps.print("\",\"nodeId\":\"");
        ps.print(nodeId);
        ps.print("\",\"startTime\":");
        ps.print(startTime);
        ps.print(",\"duration\":");
        ps.print(duration);
        ps.println("}");
    }

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op) {
        return !(ops == null || ops.get(op.id()));
    }

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op, long startTime) {
        return skip(op) || startTime < from || startTime > to;
    }

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op, int cacheId) {
        return skip(op) || !(cacheIds == null || cacheIds.contains(cacheId));
    }

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op, long startTime, int cacheId) {
        return skip(op, startTime) || !(cacheIds == null || cacheIds.contains(cacheId));
    }

    /** @return {@code True} if the operation should be skipped. */
    private boolean skip(OperationType op, long startTime, GridIntList cacheIds) {
        if (skip(op, startTime))
            return true;

        if (this.cacheIds == null)
            return false;

        GridIntIterator iter = cacheIds.iterator();

        while (iter.hasNext()) {
            if (this.cacheIds.contains(iter.next()))
                return false;
        }

        return true;
    }
}
