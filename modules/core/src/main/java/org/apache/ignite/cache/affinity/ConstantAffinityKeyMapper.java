package org.apache.ignite.cache.affinity;

/**
 * Always returns constant value as the affinity key.
 */
public class ConstantAffinityKeyMapper implements AffinityKeyMapper {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final Object constKey = 0;

    /** {@inheritDoc} */
    @Override public Object affinityKey(Object key) {
        return constKey;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // noop
    }
}
