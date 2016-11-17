package org.apache.ignite.examples;

import org.apache.ignite.examples.datagrid.SpatialQueryExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;

/**
 * Tests {@link SpatialQueryExample}.
 */
public class SpatialQueryExampleSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    public void testSpatialQueryExample() throws Exception {
        SpatialQueryExample.main(EMPTY_ARGS);
    }
}
