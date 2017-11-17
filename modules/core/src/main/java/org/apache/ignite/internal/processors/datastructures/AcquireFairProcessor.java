package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

/** {@link org.apache.ignite.cache.CacheEntryProcessor} for a acquire operation for fair mode. */
public final class AcquireFairProcessor extends ReentrantProcessor<LockOwner> {
    /** */
    private static final long serialVersionUID = 8526685073215814916L;

    /** Lock owner. */
    private LockOwner owner;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public AcquireFairProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param owner Lock owner.
     */
    AcquireFairProcessor(LockOwner owner) {
        assert owner != null;

        this.owner = owner;
    }

    /** {@inheritDoc} */
    @Override protected LockedModified lock(GridCacheLockState2Base<LockOwner> state) {
        assert state != null;

        return state.lockOrAdd(owner);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(owner.nodeId.getMostSignificantBits());
        out.writeLong(owner.nodeId.getLeastSignificantBits());
        out.writeLong(owner.threadId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        UUID nodeId = new UUID(in.readLong(), in.readLong());

        owner = new LockOwner(nodeId, in.readLong());
    }
}
