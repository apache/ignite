package org.apache.ignite.cache.database.standbycluster;

import junit.framework.TestSuite;

/**
 *
 */
public class GridChangeGlobalStateSuite extends TestSuite {
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
