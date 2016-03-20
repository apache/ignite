package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsMetaManager;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Update path closure.
 */
public final class IgfsMetaUpdatePathProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, Void>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** New path. */
    private IgfsPath path;

    /**
     * @param path Path.
     */
    public IgfsMetaUpdatePathProcessor(IgfsPath path) {
        this.path = path;
    }

    /**
     * Default constructor (required by Externalizable).
     */
    public IgfsMetaUpdatePathProcessor() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<IgniteUuid, IgfsEntryInfo> e, Object... args) {
        IgfsEntryInfo info = e.getValue();

        IgfsEntryInfo newInfo = info.path(path);

        e.setValue(newInfo);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(path);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        path = (IgfsPath)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaUpdatePathProcessor.class, this);
    }
}
