package org.apache.ignite.spark.examples;

import org.apache.ignite.examples.spark.JavaIgniteCatalogExample;
import org.apache.ignite.examples.spark.JavaIgniteDataFrameExample;
import org.apache.ignite.examples.spark.JavaIgniteDataFrameWriteExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;
import org.junit.Test;

/**
 */
public class JavaIgniteDataFrameSelfTest extends GridAbstractExamplesTest {
    static final String[] EMPTY_ARGS = new String[0];

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCatalogExample() throws Exception {
        JavaIgniteCatalogExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataFrameExample() throws Exception {
        JavaIgniteDataFrameExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataFrameWriteExample() throws Exception {
        JavaIgniteDataFrameWriteExample.main(EMPTY_ARGS);
    }
}
