package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.jetbrains.annotations.Nullable;

public class ReleaseFairProcessor implements EntryProcessor<GridCacheInternalKey, GridCacheLockState2Base<NodeThread>, NodeThread>,
    Externalizable {

    /** */
    private static final long serialVersionUID = 6727594514511280293L;

    /** */
    NodeThread owner;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public ReleaseFairProcessor() {
        // No-op.
    }

    /** */
    public ReleaseFairProcessor(NodeThread owner) {
        assert owner != null;

        this.owner = owner;
    }

    /** {@inheritDoc} */
    @Nullable @Override public NodeThread process(MutableEntry<GridCacheInternalKey, GridCacheLockState2Base<NodeThread>> entry,
        Object... objects) throws EntryProcessorException {

        assert entry != null;

        if (entry.exists()) {
            GridCacheLockState2Base<NodeThread> state = entry.getValue();

            NodeThread nextOwner = state.unlock(owner);

            // Always update value in right using.
            entry.setValue(state);

            return nextOwner;
        }

        return null;
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
        owner = new NodeThread(nodeId, in.readLong());
    }
}

