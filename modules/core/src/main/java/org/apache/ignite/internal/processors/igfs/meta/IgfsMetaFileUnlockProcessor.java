package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * File unlock entry processor.
 */
public class IgfsMetaFileUnlockProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, Void>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Modification time. */
    private long modificationTime;

    /**
     * Default constructor.
     */
    public IgfsMetaFileUnlockProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param modificationTime Modification time.
     */
    public IgfsMetaFileUnlockProcessor(long modificationTime) {
        this.modificationTime = modificationTime;
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo old = entry.getValue();

        entry.setValue(old.unlock(modificationTime));

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(modificationTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        modificationTime = in.readLong();
    }
}
