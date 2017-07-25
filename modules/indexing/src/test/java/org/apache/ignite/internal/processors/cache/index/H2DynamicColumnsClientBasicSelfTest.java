package org.apache.ignite.internal.processors.cache.index;

/**
 * Test to check {@code ALTER TABLE} operations started from client node.
 */
public class H2DynamicColumnsClientBasicSelfTest extends H2DynamicColumnsAbstractBasicSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeIndex() {
        return CLI_IDX;
    }
}
