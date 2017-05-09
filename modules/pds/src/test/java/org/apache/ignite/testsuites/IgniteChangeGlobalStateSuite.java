package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.cache.database.standbycluster.GridChangeGlobalStateCacheTest;
import org.apache.ignite.cache.database.standbycluster.GridChangeGlobalStateDataStreamerTest;
import org.apache.ignite.cache.database.standbycluster.GridChangeGlobalStateDataStructureTest;
import org.apache.ignite.cache.database.standbycluster.GridChangeGlobalStateFailOverTest;
import org.apache.ignite.cache.database.standbycluster.GridChangeGlobalStateTest;

/**
 *
 */
public class IgniteChangeGlobalStateSuite extends TestSuite {
    /**
     *
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Activate/DeActivate Cluster Test Suit");

        suite.addTestSuite(GridChangeGlobalStateTest.class);
        suite.addTestSuite(GridChangeGlobalStateCacheTest.class);
        suite.addTestSuite(GridChangeGlobalStateDataStructureTest.class);
        suite.addTestSuite(GridChangeGlobalStateDataStreamerTest.class);
        suite.addTestSuite(GridChangeGlobalStateFailOverTest.class);
//        suite.addTestSuite(GridChangeGlobalStateServiceTest.class);

        return suite;
    }
}
