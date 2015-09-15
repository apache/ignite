package org.apache.ignite.stream.storm;

import junit.framework.TestSuite;

/**
 * Apache Storm streamer tests
 */
public class IgniteStormStreamerSelfTestSuite extends TestSuite{

    public static TestSuite suite() throws Exception{

        TestSuite stormSuite = new TestSuite("Apache storm steamer test suite");

        stormSuite.addTest(new TestSuite(StormIgniteStreamerSelfTest.class));

        return stormSuite;
    }
}
