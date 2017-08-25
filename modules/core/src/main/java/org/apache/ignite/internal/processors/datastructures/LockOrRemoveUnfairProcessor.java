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

/** EntryProcessor for release lock by timeout, but acquire it if lock has released. */
public class LockOrRemoveUnfairProcessor extends ReentrantProcessor<UUID> {
    /** */
    private static final long serialVersionUID = 2968825754944751240L;

    /** */
    UUID nodeId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public LockOrRemoveUnfairProcessor() {
        // No-op.
    }

    /** */
    public LockOrRemoveUnfairProcessor(UUID nodeId) {
        assert nodeId != null;

        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override protected LockedModified tryLock(GridCacheLockState2Base<UUID> state) {
        return state.lockOrRemove(nodeId);
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
}
