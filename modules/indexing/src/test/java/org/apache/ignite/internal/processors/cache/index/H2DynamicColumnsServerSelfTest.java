package org.apache.ignite.internal.processors.cache.index;

/**
 * Test to check {@code ALTER TABLE} operations started from server node.
 */
public class H2DynamicColumnsServerSelfTest extends  H2DynamicColumnsSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeIndex() {
        return SRV_IDX;
    }
}
