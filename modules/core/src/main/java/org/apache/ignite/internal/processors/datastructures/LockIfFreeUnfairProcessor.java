package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

/** EntryProcessor for lock acquire operation. */
public class LockIfFreeUnfairProcessor extends ReentrantProcessor<UUID> {
    /** */
    private static final long serialVersionUID = -5203497119206044926L;

    /** */
    UUID nodeId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public LockIfFreeUnfairProcessor() {
        // No-op.
    }

    /** */
    public LockIfFreeUnfairProcessor(UUID nodeId) {
        assert nodeId != null;

        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(nodeId.getMostSignificantBits());
        out.writeLong(nodeId.getLeastSignificantBits());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = new UUID(in.readLong(), in.readLong());
    }

    @Override protected LockedModified tryLock(GridCacheLockState2Base<UUID> state) {
        return state.lockIfFree(nodeId);
    }
}
