package org.apache.ignite.internal.processors.database;

import org.junit.Test;

//todo remove
public class BPlusTreeReproducingTest {
    /**
     * @throws Exception
     */
    @Test
    public void testSeveralInitShutdown() throws Exception {
        BPlusTreeFakeReuseSelfTest test = new BPlusTreeFakeReuseSelfTest();

        test.beforeTest();
        try {
            test.testTestRandomPutRemoveMultithreaded_3_70_0();
        }
        finally {
            test.afterTest();
        }
    }
}
