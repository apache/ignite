package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * File lock entry processor.
 */
public class IgfsMetaFileLockProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Lock Id. */
    private IgniteUuid lockId;

    /**
     * Default constructor.
     */
    public IgfsMetaFileLockProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param lockId Lock ID.
     */
    public IgfsMetaFileLockProcessor(IgniteUuid lockId) {
        this.lockId = lockId;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo oldInfo = entry.getValue();

        IgfsEntryInfo newInfo = oldInfo.lock(lockId);

        entry.setValue(newInfo);

        return newInfo;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, lockId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        lockId = U.readGridUuid(in);
    }
}
