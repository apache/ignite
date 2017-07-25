package org.apache.ignite.internal.processors.cache.index;

/**
 * Test to check {@code ALTER TABLE} operations started from coordinator node.
 */
public class H2DynamicColumnsServerCoordinatorBasicSelfTest extends H2DynamicColumnsAbstractBasicSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeIndex() {
        return SRV_CRD_IDX;
    }
}
