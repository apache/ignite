/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.direct.singlesplit;

import org.apache.ignite.compute.*;
import org.apache.ignite.compute.gridify.*;
import org.gridgain.grid.*;
import org.gridgain.loadtests.gridify.*;

/**
 * Single split test job target.
 */
public class GridSingleSplitTestJobTarget {
    /**
     * @param level Level.
     * @param jobSes Job session.
     * @return ALways returns {@code 1}.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unused")
    @Gridify(taskClass = GridifyLoadTestTask.class, timeout = 10000)
    public int executeLoadTestJob(int level, ComputeTaskSession jobSes) throws GridException {
        assert level > 0;
        assert jobSes != null;

        jobSes.setAttribute("1st", 10000);
        jobSes.setAttribute("2nd", 10000);

        return 1;
    }
}
