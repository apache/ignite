package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.internal.processors.cache.GridCacheInternal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by ira on 4/6/15.
 */
public class CacheDataStructuresCacheKey implements GridCacheInternal, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    public CacheDataStructuresCacheKey() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 33;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj == this || (obj instanceof CacheDataStructuresCacheKey);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheDataStructuresCacheKey []";
    }
}
