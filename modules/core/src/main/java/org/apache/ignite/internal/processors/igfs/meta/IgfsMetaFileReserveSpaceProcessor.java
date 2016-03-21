package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsFileAffinityRange;
import org.apache.ignite.internal.processors.igfs.IgfsFileMap;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * File reserve space entry processor.
 */
public class IgfsMetaFileReserveSpaceProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Space. */
    private long space;

    /** Affinity range. */
    private IgfsFileAffinityRange affRange;

    /**
     * Default constructor.
     */
    public IgfsMetaFileReserveSpaceProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param space Space.
     * @param affRange
     */
    public IgfsMetaFileReserveSpaceProcessor(long space, IgfsFileAffinityRange affRange) {
        this.space = space;
        this.affRange = affRange;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo oldInfo = entry.getValue();

        IgfsFileMap newMap = new IgfsFileMap(oldInfo.fileMap());

        newMap.addRange(affRange);

        IgfsEntryInfo newInfo = oldInfo.length(oldInfo.length() + space).fileMap(newMap);

        entry.setValue(newInfo);

        return newInfo;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(space);
        out.writeObject(affRange);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        space = in.readLong();
        affRange = (IgfsFileAffinityRange)in.readObject();
    }
}
