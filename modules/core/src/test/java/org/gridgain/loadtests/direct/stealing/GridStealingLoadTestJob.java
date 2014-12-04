/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.direct.stealing;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;

import java.io.*;
import java.util.*;

/**
 * Stealing load test.
 */
public class GridStealingLoadTestJob extends ComputeJobAdapter {
    /** */
    @GridLoggerResource
    private GridLogger log;

    /** */
    @GridLocalNodeIdResource
    private UUID nodeId;

    /** */
    @GridJobContextResource
    private ComputeJobContext ctx;

    /** {@inheritDoc} */
    @Override public Serializable execute() throws GridException {
        if (log.isDebugEnabled())
            log.debug("Executing job on node [nodeId=" + nodeId + ", jobId=" + ctx.getJobId() + ']');

        try {
            Thread.sleep(500);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Here we gonna return node id which executed this job.
        // Hopefully it would be stealing node.
        return nodeId;
    }
}
