package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

/** EntryProcessor for lock acquire operation for fair mode. */
public class AcquireFairProcessor extends ReentrantProcessor<LockOwner> {
    /** */
    private static final long serialVersionUID = 8526685073215814916L;

    /** */
    LockOwner owner;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public AcquireFairProcessor() {
        // No-op.
    }

    /** */
    public AcquireFairProcessor(LockOwner owner) {
        assert owner != null;

        this.owner = owner;
    }

    /** {@inheritDoc} */
    @Override protected LockedModified tryLock(GridCacheLockState2Base<LockOwner> state) {
        return state.lockOrAdd(owner);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(owner.nodeId.getMostSignificantBits());
        out.writeLong(owner.nodeId.getLeastSignificantBits());
        out.writeLong(owner.threadId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        UUID nodeId = new UUID(in.readLong(), in.readLong());
        owner = new LockOwner(nodeId, in.readLong());
    }
}
