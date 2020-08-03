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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.List;
import java.util.Map;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpProcessor.toStringDump;

/**
 *
 */
public class ThreadPageLocksDumpLock implements PageLockDump {
    /** */
    public final long time;

    /** */
    public final Map<Integer, String> structureIdToStrcutureName;

    /** */
    public final List<ThreadState> threadStates;

    /** */
    public ThreadPageLocksDumpLock(
        long time,
        Map<Integer, String> structureIdToStrcutureName,
        List<ThreadState> threadStates
    ) {
        this.time = time;
        this.structureIdToStrcutureName = structureIdToStrcutureName;
        this.threadStates = threadStates;
    }

    /** */
    public static class ThreadState {
        /** */
        public final long threadId;

        /** */
        public final String threadName;

        /** */
        public final Thread.State state;

        /** */
        public final PageLockDump pageLockDump;

        /** */
        public final InvalidContext<? extends PageLockDump> invalidContext;

        /** */
        public ThreadState(
            long threadId,
            String threadName,
            Thread.State state,
            PageLockDump pageLockDump,
            InvalidContext<? extends PageLockDump> invalidContext
        ) {
            this.threadId = threadId;
            this.threadName = threadName;
            this.state = state;
            this.pageLockDump = pageLockDump;
            this.invalidContext = invalidContext;
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
