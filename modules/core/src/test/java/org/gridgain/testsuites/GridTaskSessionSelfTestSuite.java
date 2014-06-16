/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.session.*;

/**
 * Task session test suite.
 */
public class GridTaskSessionSelfTestSuite extends TestSuite {
    /**
     * @return TaskSession test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain TaskSession Test Suite");

        suite.addTest(new TestSuite(GridSessionCancelSiblingsFromFutureSelfTest.class));
        suite.addTest(new TestSuite(GridSessionCancelSiblingsFromJobSelfTest.class));
        suite.addTest(new TestSuite(GridSessionCancelSiblingsFromTaskSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetFutureAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetFutureAttributeWaitListenerSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetJobAttributeWaitListenerSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetJobAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetJobAttribute2SelfTest.class));
        suite.addTest(new TestSuite(GridSessionJobWaitTaskAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetTaskAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionFutureWaitTaskAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionFutureWaitJobAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionTaskWaitJobAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetJobAttributeOrderSelfTest.class));
        suite.addTest(new TestSuite(GridSessionWaitAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionJobFailoverSelfTest.class));
        suite.addTest(new TestSuite(GridSessionLoadSelfTest.class));
        suite.addTest(new TestSuite(GridSessionCollisionSpiSelfTest.class));
        suite.addTest(new TestSuite(GridSessionCheckpointSelfTest.class));

        return suite;
    }
}
