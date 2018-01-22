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

package org.apache.ignite.internal.processors.query.h2.views;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: JVM threads.
 */
public class GridH2SysViewImplJvmThreads extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplJvmThreads(GridKernalContext ctx) {
        super("JVM_THREADS", "JVM threads", ctx, "ID",
            newColumn("ID"),
            newColumn("NAME"),
            newColumn("STATE"),
            newColumn("IS_DEADLOCKED", Value.BOOLEAN),
            newColumn("BLOCKED_COUNT", Value.LONG),
            newColumn("BLOCKED_TIME", Value.TIME),
            newColumn("WAITED_COUNT", Value.LONG),
            newColumn("WAITED_TIME", Value.TIME),
            newColumn("LOCK_NAME"),
            newColumn("LOCK_OWNER_ID", Value.LONG),
            newColumn("LOCK_OWNER_NAME"),
            newColumn("LOCKED_MONITORS"),
            newColumn("LOCKED_SYNCHRONIZERS"),
            newColumn("STACK_TRACE")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

        long[] deadlockedIds = mxBean.findDeadlockedThreads();

        Set<Long> deadlockedThreads = new HashSet<>();

        if (deadlockedIds != null) {
            for (Long threadId : deadlockedIds)
                deadlockedThreads.add(threadId);
        }

        ThreadInfo[] threads;

        ColumnCondition idCond = conditionForColumn("ID", first, last);

        if (idCond.isEquality()) {
            log.debug("Get threads: thread id");

            threads = mxBean.getThreadInfo(new long[] { idCond.getValue().getLong() },
                mxBean.isObjectMonitorUsageSupported(), mxBean.isSynchronizerUsageSupported());
        }
        else {
            log.debug("Get threads: full scan");

            threads = mxBean.dumpAllThreads(mxBean.isObjectMonitorUsageSupported(),
                mxBean.isSynchronizerUsageSupported());
        }

        for (ThreadInfo thread : threads) {
            if (thread != null)
                rows.add(
                    createRow(ses, rows.size(),
                        thread.getThreadId(),
                        thread.getThreadName(),
                        thread.getThreadState(),
                        deadlockedThreads.contains(thread.getThreadId()),
                        thread.getBlockedCount(),
                        valueTimeFromMillis(thread.getBlockedTime()),
                        thread.getWaitedCount(),
                        valueTimeFromMillis(thread.getWaitedTime()),
                        thread.getLockName(),
                        thread.getLockOwnerId(),
                        thread.getLockOwnerName(),
                        Arrays.toString(thread.getLockedMonitors()),
                        Arrays.toString(thread.getLockedSynchronizers()),
                        stackTraceToString(thread.getStackTrace())
                    )
                );
        }

        return rows;
    }

    /**
     * Converts stack trace to string.
     *
     * @param stackTrace Stack trace.
     */
    private String stackTraceToString(StackTraceElement[] stackTrace) {
        StringBuilder sb = new StringBuilder();

        for (StackTraceElement element : stackTrace ){
            sb.append(element);
            sb.append(U.nl());
        }

        return sb.toString();
    }
}
