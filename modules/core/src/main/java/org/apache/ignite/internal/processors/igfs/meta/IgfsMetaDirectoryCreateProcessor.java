package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsListingEntry;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;

/**
 * Directory create processor.
 */
public class IgfsMetaDirectoryCreateProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Create time. */
    private long createTime;

    /** Properties. */
    private Map<String, String> props;

    /** Child name (optional). */
    private String childName;

    /** Child entry (optional. */
    private IgfsListingEntry childEntry;

    /**
     * Constructor.
     */
    public IgfsMetaDirectoryCreateProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param createTime Create time.
     * @param props Properties.
     */
    public IgfsMetaDirectoryCreateProcessor(long createTime, Map<String, String> props) {
        this(createTime, props, null, null);
    }

    /**
     * Constructor.
     *
     * @param createTime Create time.
     * @param props Properties.
     * @param childName Child name.
     * @param childEntry Child entry.
     */
    public IgfsMetaDirectoryCreateProcessor(long createTime, Map<String, String> props, String childName,
        IgfsListingEntry childEntry) {
        this.createTime = createTime;
        this.props = props;
        this.childName = childName;
        this.childEntry = childEntry;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {

        IgfsEntryInfo info = IgfsUtils.createDirectory(
            entry.getKey(),
            null,
            props,
            createTime,
            createTime
        );

        if (childName != null)
            info = info.listing(Collections.singletonMap(childName, childEntry));

        entry.setValue(info);

        return info;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(createTime);
        U.writeStringMap(out, props);

        if (childName != null) {
            out.writeBoolean(true);

            U.writeString(out, childName);
            out.writeObject(childEntry);
        }
        else
            out.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        createTime = in.readLong();
        props = U.readStringMap(in);

        if (in.readBoolean()) {
            childName = U.readString(in);
            childEntry = (IgfsListingEntry)in.readObject();
        }
    }
}
