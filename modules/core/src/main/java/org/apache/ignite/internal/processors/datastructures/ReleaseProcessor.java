package org.apache.ignite.internal.processors.datastructures;

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

/** EntryProcessor for lock release operation. */
public class ReleaseProcessor implements EntryProcessor<GridCacheInternalKey, GridCacheLockState2, UUID>,
    Binarylizable, Serializable {

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    UUID nodeId;

    /** */
    public ReleaseProcessor() {
        // No-op.
    }

    /** */
    public ReleaseProcessor(UUID nodeId) {
        assert nodeId != null;

        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public UUID process(MutableEntry<GridCacheInternalKey, GridCacheLockState2> entry,
        Object... objects) throws EntryProcessorException {

        assert entry != null;

        if (entry.exists()) {
            GridCacheLockState2 state = entry.getValue();

            UUID nextNode = state.unlock(nodeId);

            // Always update value in right using.
            entry.setValue(state);

            return nextNode;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        final BinaryRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeLong(nodeId.getMostSignificantBits());
        rawWriter.writeLong(nodeId.getLeastSignificantBits());
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        final BinaryRawReader rawReader = reader.rawReader();

        nodeId = new UUID(rawReader.readLong(), rawReader.readLong());
    }
}
