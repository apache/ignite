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
 * Update range processor.
 */
public class IgfsMetaFileRangeUpdateProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Range. */
    private IgfsFileAffinityRange range;

    /** Status. */
    private int status;

    /**
     * Constructor.
     */
    public IgfsMetaFileRangeUpdateProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param range Range.
     * @param status Status.
     */
    public IgfsMetaFileRangeUpdateProcessor(IgfsFileAffinityRange range, int status) {
        this.range = range;
        this.status = status;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo oldInfo = entry.getValue();

        IgfsFileMap newMap = new IgfsFileMap(oldInfo.fileMap());

        newMap.updateRangeStatus(range, status);

        IgfsEntryInfo newInfo = oldInfo.fileMap(newMap);

        entry.setValue(newInfo);

        return newInfo;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(range);
        out.writeInt(status);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        range = (IgfsFileAffinityRange)in.readObject();
        status = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaFileRangeUpdateProcessor.class, this);
    }
}
