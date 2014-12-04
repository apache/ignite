/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.future.nio.*;

/**
 * Gridgain language test suite.
 */
public class GridLangSelfTestSuite extends TestSuite {
    /**
     * @return Kernal test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Lang Test Suite");

        suite.addTest(new TestSuite(GridFuncSelfTest.class));
        suite.addTest(new TestSuite(GridTupleSelfTest.class));
        suite.addTest(new TestSuite(GridByteArrayListSelfTest.class));
        suite.addTest(new TestSuite(GridLeanMapSelfTest.class));
        suite.addTest(new TestSuite(GridListSetSelfTest.class));
        suite.addTest(new TestSuite(GridSetWrapperSelfTest.class));
        suite.addTest(new TestSuite(GridConcurrentWeakHashSetSelfTest.class));
        suite.addTest(new TestSuite(GridMetadataAwareAdapterSelfTest.class));
        suite.addTest(new TestSuite(GridSetWrapperSelfTest.class));
        suite.addTest(new TestSuite(IgniteUuidSelfTest.class));
        suite.addTest(new TestSuite(GridXSelfTest.class));
        suite.addTest(new TestSuite(GridBoundedConcurrentOrderedMapSelfTest.class));
        suite.addTest(new TestSuite(GridBoundedConcurrentLinkedHashMapSelfTest.class));
        suite.addTest(new TestSuite(GridConcurrentLinkedDequeSelfTest.class));
        suite.addTest(new TestSuite(GridCircularBufferSelfTest.class));
        suite.addTest(new TestSuite(GridConcurrentLinkedHashMapSelfTest.class));
        suite.addTest(new TestSuite(GridConcurrentLinkedHashMapMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridCacheConcurrentMapSelfTest.class));
        suite.addTest(new TestSuite(GridStripedLockSelfTest.class));

        suite.addTest(new TestSuite(GridFutureAdapterSelfTest.class));
        suite.addTest(new TestSuite(GridFinishedFutureSelfTest.class));
        suite.addTest(new TestSuite(GridCompoundFutureSelfTest.class));
        suite.addTest(new TestSuite(GridEmbeddedFutureSelfTest.class));
        suite.addTest(new TestSuite(GridNioFutureSelfTest.class));
        suite.addTest(new TestSuite(GridNioEmbeddedFutureSelfTest.class));

        // Consistent hash tests.
        suite.addTest(new TestSuite(GridConsistentHashSelfTest.class));

        return suite;
    }
}
