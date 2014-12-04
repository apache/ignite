/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.direct.newnodes;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;

/**
 * Single split on new nodes test job target.
 */
public class GridSingleSplitNewNodesTestJobTarget {
    /**
     * @param level Level.
     * @param jobSes Job session.
     * @return Always returns {@code 1}.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unused")
    public int executeLoadTestJob(int level, GridComputeTaskSession jobSes) throws GridException {
        assert level > 0;
        assert jobSes != null;

        try {
            assert "1".equals(jobSes.waitForAttribute("1st", 10000));

            assert "2".equals(jobSes.waitForAttribute("2nd", 10000));
        }
        catch (InterruptedException e) {
            // Fail.
            throw new GridException("Failed to wait for attribute.", e);
        }

        return 1;
    }
}
