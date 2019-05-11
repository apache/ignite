package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.Dump;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.DumpProcessor;

import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

public class LockStackSnapshot implements Dump {
    public final String name;

    public final long time;

    public final int headIdx;

    public final long[] pageIdLocksStack;

    public final int nextOp;
    public final int nextOpStructureId;
    public final long nextOpPageId;

    public LockStackSnapshot(
        String name,
        long time,
        int headIdx,
        long[] pageIdLocksStack,
        int nextOp,
        int nextOpStructureId,
        long nextOpPageId
    ) {
        this.name = name;
        this.time = time;
        this.headIdx = headIdx;
        this.pageIdLocksStack = pageIdLocksStack;
        this.nextOp = nextOp;
        this.nextOpStructureId = nextOpStructureId;
        this.nextOpPageId = nextOpPageId;
    }

    @Override public void apply(DumpProcessor dumpProcessor) {
        dumpProcessor.processDump(this);
    }
}
