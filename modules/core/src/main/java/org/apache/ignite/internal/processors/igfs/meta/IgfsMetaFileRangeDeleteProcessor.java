package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsFileAffinityRange;
import org.apache.ignite.internal.processors.igfs.IgfsFileMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Delete range processor.
 */
public class IgfsMetaFileRangeDeleteProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Range. */
    private IgfsFileAffinityRange range;

    /**
     * Constructor.
     */
    public IgfsMetaFileRangeDeleteProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param range Range.
     */
    public IgfsMetaFileRangeDeleteProcessor(IgfsFileAffinityRange range) {
        this.range = range;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo oldInfo = entry.getValue();

        IgfsFileMap newMap = new IgfsFileMap(oldInfo.fileMap());

        newMap.deleteRange(range);

        IgfsEntryInfo newInfo = oldInfo.fileMap(newMap);

        entry.setValue(newInfo);

        return newInfo;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(range);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        range = (IgfsFileAffinityRange)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaFileRangeDeleteProcessor.class, this);
    }
}
