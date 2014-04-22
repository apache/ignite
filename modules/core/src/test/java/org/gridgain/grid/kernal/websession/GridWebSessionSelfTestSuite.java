/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.websession;

import junit.framework.*;

/**
 * Test suite for web sessions caching functionality.
 */
@SuppressWarnings("PublicInnerClass")
public class GridWebSessionSelfTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("GridGain Web Sessions Test Suite");

        suite.addTestSuite(GridWebSessionSelfTest.class);
        suite.addTestSuite(WebSessionTransactionalSelfTest.class);
        suite.addTestSuite(WebSessionReplicatedSelfTest.class);

        return suite;
    }

    /**
     * Tests web sessions with TRANSACTIONAL cache.
     */
    public static class WebSessionTransactionalSelfTest extends GridWebSessionSelfTest {
        /** {@inheritDoc} */
        @Override protected String getCacheName() {
            return "partitioned_tx";
        }

        /** {@inheritDoc} */
        @Override public void testRestarts() throws Exception {
            // TODO GG-8166, enable when fixed.
        }
    }

    /**
     * Tests web sessions with REPLICATED cache.
     */
    public static class WebSessionReplicatedSelfTest extends GridWebSessionSelfTest {
        /** {@inheritDoc} */
        @Override protected String getCacheName() {
            return "replicated";
        }
    }
}
