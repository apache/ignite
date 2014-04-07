/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.spi.indexing.h2.*;
import org.gridgain.grid.spi.indexing.h2.opt.*;
import org.gridgain.grid.util.offheap.unsafe.*;

/**
 * Indexing SPI tests.
 */
public class GridSpiIndexingSelfTestSuite extends TestSuite {
    /**
     * @return Failover SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Indexing SPI Test Suite");

        // H2 Optimized table test.
        suite.addTest(new TestSuite(GridH2TableSelfTest.class));

        // H2 Indexing in-memory.
        suite.addTest(new TestSuite(GridH2IndexingSpiInMemStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridH2IndexingSpiInMemSelfTest.class));

        // H2 Off-heap memory.
        suite.addTest(new TestSuite(GridH2IndexingSpiOffheapStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridH2IndexingSpiOffheapSelfTest.class));
        suite.addTest(new TestSuite(GridOffheapSnapTreeSelfTest.class));

        // Result set iterator.
        suite.addTest(new TestSuite(GridH2ResultSetIteratorSelfTest.class));

        // Index rebuilding.
        suite.addTest(new TestSuite(GridH2IndexRebuildTest.class));

        // Geo.
        suite.addTestSuite(GridH2IndexingSpiGeoSelfTest.class);

        return suite;
    }
}
