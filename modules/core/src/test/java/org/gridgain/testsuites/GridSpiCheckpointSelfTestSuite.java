/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.spi.checkpoint.cache.*;
import org.gridgain.grid.spi.checkpoint.jdbc.*;
import org.gridgain.grid.spi.checkpoint.sharedfs.*;

/**
 * Grid SPI checkpoint self test suite.
 */
public class GridSpiCheckpointSelfTestSuite extends TestSuite {
    /**
     * @return Checkpoint test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Checkpoint Test Suite");

        // Cache.
        suite.addTest(new TestSuite(GridCacheCheckpointSpiConfigSelfTest.class));
        suite.addTest(new TestSuite(GridCacheCheckpointSpiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheCheckpointSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridCacheCheckpointSpiSecondCacheSelfTest.class));

        // JDBC.
        suite.addTest(new TestSuite(GridJdbcCheckpointSpiConfigSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcCheckpointSpiCustomConfigSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcCheckpointSpiDefaultConfigSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcCheckpointSpiStartStopSelfTest.class));

        // Shared FS.
        suite.addTest(new TestSuite(GridSharedFsCheckpointSpiMultipleDirectoriesSelfTest.class));
        suite.addTest(new TestSuite(GridSharedFsCheckpointSpiSelfTest.class));
        suite.addTest(new TestSuite(GridSharedFsCheckpointSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridSharedFsCheckpointSpiConfigSelfTest.class));

        return suite;
    }
}
