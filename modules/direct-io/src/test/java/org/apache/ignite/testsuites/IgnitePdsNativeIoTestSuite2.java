package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.testsuites.IgnitePdsTestSuite;
import org.apache.ignite.testsuites.IgnitePdsTestSuite2;

/**
 * Same as {@link IgnitePdsTestSuite2} but is started with direct-oi jar in classpath.
 */
public class IgnitePdsNativeIoTestSuite2 extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Persistent Store Test Suite");

        suite.addTest(IgnitePdsTestSuite2.suite());

        return suite;
    }
}
