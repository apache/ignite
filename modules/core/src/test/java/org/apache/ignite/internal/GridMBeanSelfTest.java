package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Grid MBean self test
 */
@GridCommonTest(group = "Kernal Self")
public class GridMBeanSelfTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMBeanReturnClassType() throws Exception{
        Ignite ignite = grid();
        ((IgniteKernal)ignite).getUserAttributesFormatted();
    }
}
