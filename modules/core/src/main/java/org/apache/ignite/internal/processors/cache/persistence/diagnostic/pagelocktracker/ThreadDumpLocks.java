package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.List;
import java.util.Map;

public class ThreadDumpLocks implements Dump {
    public final Map<Integer, String> structureIdToStrcutureName;

    public final List<ThreadState> threadStates;

    public ThreadDumpLocks(
        Map<Integer, String> structureIdToStrcutureName,
        List<ThreadState> threadStates
    ) {
        this.structureIdToStrcutureName = structureIdToStrcutureName;
        this.threadStates = threadStates;
    }

    public static class ThreadState {
        public final long threadId;
        public final String threadName;
        public final Thread.State state;

        public final Dump dump;

        public final InvalidContext<Dump> invalidContext;

        public ThreadState(
            long threadId,
            String threadName,
            Thread.State state,
            Dump dump,
            InvalidContext<Dump> invalidContext
        ) {
            this.threadId = threadId;
            this.threadName = threadName;
            this.state = state;
            this.dump = dump;
            this.invalidContext = invalidContext;
        }
    }

    @Override public void apply(DumpProcessor dumpProcessor) {
        dumpProcessor.processDump(this);
    }
}
