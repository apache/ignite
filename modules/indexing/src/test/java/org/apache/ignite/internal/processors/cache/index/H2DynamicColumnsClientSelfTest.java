package org.apache.ignite.internal.processors.cache.index;

/**
 * Test to check {@code ALTER TABLE} operations started from client node.
 */
public class H2DynamicColumnsClientSelfTest extends  H2DynamicColumnsSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeIndex() {
        return CLI_IDX;
    }
}
