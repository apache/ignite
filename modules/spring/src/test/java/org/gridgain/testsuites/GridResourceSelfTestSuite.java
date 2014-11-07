/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.kernal.processors.resource.*;

/**
 * Gridgain resource injection test Suite.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
public class GridResourceSelfTestSuite extends TestSuite {
    /**
     * @return Resource injection test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Resource Injection Test Suite");

        suite.addTest(new TestSuite(GridResourceFieldInjectionSelfTest.class));
        suite.addTest(new TestSuite(GridResourceFieldOverrideInjectionSelfTest.class));
        suite.addTest(new TestSuite(GridResourceMethodInjectionSelfTest.class));
        suite.addTest(new TestSuite(GridResourceMethodOverrideInjectionSelfTest.class));
        suite.addTest(new TestSuite(GridResourceProcessorSelfTest.class));
        suite.addTest(new TestSuite(GridResourceIsolatedTaskSelfTest.class));
        suite.addTest(new TestSuite(GridResourceIsolatedClassLoaderSelfTest.class));
        suite.addTest(new TestSuite(GridResourceSharedUndeploySelfTest.class));
        suite.addTest(new TestSuite(GridResourceUserExternalTest.class));
        suite.addTest(new TestSuite(GridResourceEventFilterSelfTest.class));
        suite.addTest(new TestSuite(GridLoggerInjectionSelfTest.class));
        suite.addTest(new TestSuite(GridResourceConcurrentUndeploySelfTest.class));
        suite.addTest(new TestSuite(GridResourceIocSelfTest.class));

        return suite;
    }
}
