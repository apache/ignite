package org.apache.ignite.stream.akka;

import junit.framework.TestSuite;

/**
 * Akka-Ignite unit tests
 */
public class IgniteAkkaStreamerTestSuite extends TestSuite {
    /**
     *
     * @return TestSuite
     * @throws Exception thrown for test failues
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Akka streamer Test Suite");

        // Akka ignite streamer
        suite.addTest(new TestSuite(AkkaIgniteStreamerTest.class));

        return suite;
    }
}
