package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * File create processor.
 */
public class IgfsMetaFileCreateProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Create time. */
    private long createTime;

    /** Properties. */
    private Map<String, String> props;

    /** Block size. */
    private int blockSize;

    /** Affintiy key. */
    private IgniteUuid affKey;

    /** Lcok ID. */
    private IgniteUuid lockId;

    /** Evict exclude flag. */
    private boolean evictExclude;

    /**
     * Constructor.
     */
    public IgfsMetaFileCreateProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param createTime Create time.
     * @param props Properties.
     * @param blockSize Block size.
     * @param affKey Affinity key.
     * @param lockId Lock ID.
     * @param evictExclude Evict exclude flag.
     */
    public IgfsMetaFileCreateProcessor(long createTime, Map<String, String> props, int blockSize,
        @Nullable IgniteUuid affKey, IgniteUuid lockId, boolean evictExclude) {
        this.createTime = createTime;
        this.props = props;
        this.blockSize = blockSize;
        this.affKey = affKey;
        this.lockId = lockId;
        this.evictExclude = evictExclude;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo info = IgfsUtils.createFile(
            entry.getKey(),
            blockSize,
            0L,
            affKey,
            lockId,
            evictExclude,
            props,
            createTime,
            createTime
        );

        entry.setValue(info);

        return info;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(createTime);
        U.writeStringMap(out, props);
        out.writeInt(blockSize);
        out.writeObject(affKey);
        out.writeObject(lockId);
        out.writeBoolean(evictExclude);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        createTime = in.readLong();
        props = U.readStringMap(in);
        blockSize = in.readInt();
        affKey = (IgniteUuid)in.readObject();
        lockId = (IgniteUuid)in.readObject();
        evictExclude = in.readBoolean();
    }
}
