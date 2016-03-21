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
 * Update times entry processor.
 */
public class IgfsMetaUpdateTimesProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, Void>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Access time. */
    private long accessTime;

    /** Modification time. */
    private long modificationTime;

    /**
     * Default constructor.
     */
    public IgfsMetaUpdateTimesProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param accessTime Access time.
     * @param modificationTime Modification time.
     */
    public IgfsMetaUpdateTimesProcessor(long accessTime, long modificationTime) {
        this.accessTime = accessTime;
        this.modificationTime = modificationTime;
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {

        IgfsEntryInfo oldInfo = entry.getValue();

        entry.setValue(oldInfo.accessModificationTime(accessTime, modificationTime));

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(accessTime);
        out.writeLong(modificationTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        accessTime = in.readLong();
        modificationTime = in.readLong();
    }
}
