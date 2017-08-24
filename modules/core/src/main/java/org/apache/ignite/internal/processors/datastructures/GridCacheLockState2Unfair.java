package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

public class GridCacheLockState2Unfair extends GridCacheLockState2Base<UUID> {
    /** */
    private static final long serialVersionUID = 6727594514511280291L;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheLockState2Unfair() {
        super();
    }

    /**
     * Constructor.
     *
     * @param gridStartTime Cluster start time.
     */
    public GridCacheLockState2Unfair(long gridStartTime) {
        super(gridStartTime);
    }

    /** Clone constructor. */
    protected GridCacheLockState2Unfair(GridCacheLockState2Base<UUID> state) {
        super(state);
    }

    /** {@inheritDoc} */
    @Override protected Object clone(){
        return new GridCacheLockState2Unfair(this);
    }

    /** {@inheritDoc} */
    @Override public void writeItem(ObjectOutput out, UUID item) throws IOException {
        out.writeLong(item.getMostSignificantBits());
        out.writeLong(item.getLeastSignificantBits());
    }

    /** {@inheritDoc} */
    @Override public UUID readItem(ObjectInput in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }
}
