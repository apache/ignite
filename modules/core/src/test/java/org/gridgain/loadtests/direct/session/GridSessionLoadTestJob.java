/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.direct.session;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;

import java.io.*;

/**
 * Session load test job.
 */
public class GridSessionLoadTestJob extends ComputeJobAdapter {
    /** */
    @IgniteTaskSessionResource
    private ComputeTaskSession taskSes;

    /** */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** */
    public GridSessionLoadTestJob() {
        // No-op.
    }

    /**
     * @param arg Argument.
     */
    public GridSessionLoadTestJob(String arg) {
        super(arg);
    }

    /** {@inheritDoc} */
    @Override public Serializable execute() throws GridException {
        assert taskSes != null;

        Object arg = argument(0);

        assert arg != null;

        Serializable ser = taskSes.getAttribute(arg);

        assert ser != null;

        int val = (Integer)ser + 1;

        // Generate garbage.
        for (int i = 0; i < 10; i++)
            taskSes.setAttribute(arg, i);

        // Set final value.
        taskSes.setAttribute(arg, val);

        if (log.isDebugEnabled())
            log.debug("Set session attribute [name=" + arg + ", value=" + val + ']');

        return val;
    }
}
