package org.apache.ignite.internal.processors.datastructures;

/** Simple tuple for result. */
public class LockedModified {
    /** */
    public boolean locked;

    /** */
    public boolean modified;

    /** */
    public LockedModified(boolean locked, boolean modified) {
        this.locked = locked;
        this.modified = modified;
    }
}
