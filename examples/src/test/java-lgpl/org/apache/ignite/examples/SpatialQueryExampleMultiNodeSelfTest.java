package org.apache.ignite.examples;

import org.apache.ignite.examples.datagrid.SpatialQueryExample;

/**
 * * Tests {@link SpatialQueryExample} in the multi node mode.
 */
public class SpatialQueryExampleMultiNodeSelfTest extends SpatialQueryExampleSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < RMT_NODES_CNT; i++)
            startGrid("node-" + i, "examples/config/example-ignite.xml");
    }
}
