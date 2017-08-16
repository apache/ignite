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

/** EntryProcessor for release lock by timeout, but acquire it if lock has released. */
public class LockOrRemoveProcessor implements EntryProcessor<GridCacheInternalKey, GridCacheLockState2, Boolean>,
    Binarylizable, Serializable {

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    UUID nodeId;

    /** */
    public LockOrRemoveProcessor(UUID nodeId) {
        assert nodeId != null;

        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public Boolean process(MutableEntry<GridCacheInternalKey, GridCacheLockState2> entry,
        Object... objects) throws EntryProcessorException {

        assert entry != null;

        if (entry.exists()) {
            GridCacheLockState2 state = entry.getValue();

            GridCacheLockState2.LockedModified result = state.lockOrRemove(nodeId);

            // Write result if necessary.
            if (result.modified)
                entry.setValue(state);

            return result.locked;
        }

        return false;
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
