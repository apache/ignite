package org.apache.ignite.internal.processors.cache.database.pagemem;

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class EvictCandidate {
    /** */
    private int tag;

    /** */
    @GridToStringExclude
    private long relPtr;

    /** */
    @GridToStringInclude
    private FullPageId fullId;

    /**
     * @param tag Tag.
     * @param relPtr Relative pointer.
     * @param fullId Full page ID.
     */
    public EvictCandidate(int tag, long relPtr, FullPageId fullId) {
        this.tag = tag;
        this.relPtr = relPtr;
        this.fullId = fullId;
    }

    /**
     * @return Tag.
     */
    public int tag() {
        return tag;
    }

    /**
     * @return Relative pointer.
     */
    public long relativePointer() {
        return relPtr;
    }

    /**
     * @return Index.
     */
    public FullPageId fullId() {
        return fullId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(EvictCandidate.class, this, "relPtr", U.hexLong(relPtr));
    }
}
