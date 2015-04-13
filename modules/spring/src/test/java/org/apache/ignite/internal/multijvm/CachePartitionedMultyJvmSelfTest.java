/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.multijvm;

/**
 * Test cases for multi-jvm tests.
 */
public class CachePartitionedMultyJvmSelfTest extends MultiJvmTest{
    private static final String IGNITE_NODE_1 = "IGNITE_NODE_1";
    private static final String IGNITE_NODE_2 = "IGNITE_NODE_2";
    private static final String IGNITE_NODE_3 = "IGNITE_NODE_3";
    private static final String CFG = "modules/spring/src/test/java/org/apache/ignite/internal/multijvm/example-cache.xml";

    @Override protected void beforeTestsStarted() throws Exception {
        runIgniteProcess(IGNITE_NODE_1, CFG);
        runIgniteProcess(IGNITE_NODE_2, CFG);
        runIgniteProcess(IGNITE_NODE_3, CFG);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiJvmPut() throws Exception {
        executeTaskAndWaitForFinish(IGNITE_NODE_1, PutInCache.class, 1);
        
        executeTaskAndWaitForFinish(IGNITE_NODE_1, CheckInCache.class, 1);

        executeTaskAndWaitForFinish(IGNITE_NODE_2, CheckInCache.class, 1);
        
        executeTaskAndWaitForFinish(IGNITE_NODE_3, CheckInCache.class, 1);
    }
}
