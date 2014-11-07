/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.gridify;

import org.gridgain.grid.compute.gridify.*;

/**
 * Gridify load test job target.
 */
public class GridifyLoadTestJobTarget {
    /**
     * @param level Level.
     * @return Always returns {@code 1}.
     */
    @SuppressWarnings("unused")
    @Gridify(taskClass = GridifyLoadTestTask.class, timeout = 20000)
    public int executeLoadTestJob(int level) {
        assert level > 0;

        return 1;
    }
}
