/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2;

/**
 * Tests for H2 indexing SPI.
 */
public class GridH2IndexingOffheapSelfTest extends GridIndexingSpiAbstractSelfTest {
    /** */
    private static final long offheap = 10000000;

    private static GridH2Indexing currentSpi;

    /** {@inheritDoc} */
    @Override protected void startIndexing(GridH2Indexing spi) throws Exception {
        spi.configuration().setMaxOffHeapMemory(offheap);

        currentSpi = spi;

        super.startIndexing(spi);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        assertEquals(0, currentSpi.getAllocatedOffHeapMemory());
    }
}
