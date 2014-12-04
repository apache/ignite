/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.logger.*;

/**
 * Test job.
 */
public class GridTestJob extends ComputeJobAdapter {
    /** Logger. */
    @GridLoggerResource
    private GridLogger log;

    /** */
    public GridTestJob() {
        // No-op.
    }

    /**
     * @param arg Job argument.
     */
    public GridTestJob(String arg) {
        super(arg);
    }

    /** {@inheritDoc} */
    @Override public String execute() throws GridException {
        if (log.isDebugEnabled())
            log.debug("Executing job [job=" + this + ", arg=" + argument(0) + ']');

        return argument(0);
    }
}
