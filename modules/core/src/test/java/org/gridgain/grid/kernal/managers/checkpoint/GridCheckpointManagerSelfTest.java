/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.checkpoint;

import org.gridgain.testframework.junits.common.*;

/**
 *
 */
@GridCommonTest(group = "Checkpoint Manager")
public class GridCheckpointManagerSelfTest extends GridCheckpointManagerAbstractSelfTest {
    /**
     * @throws Exception Thrown if any exception occurs.
     */
    public void testCacheBased() throws Exception {
        doTest("cache");
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    public void testSharedFsBased() throws Exception {
        doTest("sharedfs");
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    public void testDatabaseBased() throws Exception {
        doTest("jdbc");
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    public void testMultiNodeCacheBased() throws Exception {
        doMultiNodeTest("cache");
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    public void testMultiNodeSharedFsBased() throws Exception {
        doMultiNodeTest("sharedfs");
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    public void testMultiNodeDatabaseBased() throws Exception {
        doMultiNodeTest("jdbc");
    }
}
