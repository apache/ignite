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

/** EntryProcessor for lock release operation. */
public class ReleaseUnfairProcessor implements EntryProcessor<GridCacheInternalKey, GridCacheLockState2Unfair, UUID>,
    Externalizable {

    /** */
    private static final long serialVersionUID = 6727594514511280293L;

    /** */
    UUID nodeId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public ReleaseUnfairProcessor() {
        // No-op.
    }

    /** */
    public ReleaseUnfairProcessor(UUID nodeId) {
        assert nodeId != null;

        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID process(MutableEntry<GridCacheInternalKey, GridCacheLockState2Unfair> entry,
        Object... objects) throws EntryProcessorException {

        assert entry != null;

        if (entry.exists()) {
            GridCacheLockState2Unfair state = entry.getValue();

            UUID nextId = state.unlock(nodeId);

            // Always update value in right using.
            entry.setValue(state);

            return nextId;
        }

        return null;
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
