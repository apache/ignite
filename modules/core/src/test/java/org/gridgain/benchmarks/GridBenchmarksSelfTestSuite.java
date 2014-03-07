/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.benchmarks;

import junit.framework.*;
import org.gridgain.benchmarks.risk.*;
import org.gridgain.benchmarks.risk.affinity.*;
import org.gridgain.benchmarks.risk.jobs.*;
import org.gridgain.benchmarks.risk.store.*;
import org.gridgain.testframework.*;

/**
 * All benchmarks test suite.
 */
public class GridBenchmarksSelfTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = GridTestUtils.createLocalTestSuite(
            "GridGain Benchmarks Test Suite");

        // Risk analytics benchmark tests.
        suite.addTest(new TestSuite(GridRiskMainSelfTest.class));
        suite.addTest(new TestSuite(GridRiskPartitionedAffinitySelfTest.class));
        suite.addTest(new TestSuite(GridRiskMetricCalculationSelfTest.class));
        suite.addTest(new TestSuite(GridRiskCacheStoreSelfTest.class));

        // Serialization benchmark tests.
        suite.addTest(new TestSuite(GridSerializationBenchmarkSelfTest.class));

        return suite;
    }
}
