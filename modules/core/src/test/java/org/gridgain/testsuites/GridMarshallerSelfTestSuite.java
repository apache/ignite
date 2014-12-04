/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.grid.marshaller.jdk.*;
import org.gridgain.grid.util.io.*;

/**
 * Test suite for all marshallers.
 */
public class GridMarshallerSelfTestSuite extends TestSuite {
    /**
     * @return Kernal test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Marshaller Test Suite");

        suite.addTest(new TestSuite(GridJdkMarshallerSelfTest.class));
        suite.addTest(new TestSuite(GridOptimizedMarshallerEnumSelfTest.class));
        suite.addTest(new TestSuite(GridOptimizedMarshallerSelfTest.class));
        suite.addTest(new TestSuite(GridOptimizedMarshallerTest.class));
        suite.addTest(new TestSuite(GridOptimizedObjectStreamSelfTest.class));
        suite.addTest(new TestSuite(GridUnsafeDataOutputArraySizingSelfTest.class));

        return suite;
    }
}
