/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.tests.p2p;

/**
 * Test class for p2p chained deployment.
 */
public class GridSingleSplitTestJobTarget {
    /**
     * @param level Test argument.
     * @return Always 1.
     */
    @SuppressWarnings("unused")
    public int executeLoadTestJob(int level) {
        assert level > 0;

        return 1;
    }
}
