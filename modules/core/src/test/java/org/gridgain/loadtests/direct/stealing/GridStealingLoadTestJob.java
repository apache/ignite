/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.direct.stealing;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;

import java.io.*;
import java.util.*;

/**
 * Stealing load test.
 */
public class GridStealingLoadTestJob extends ComputeJobAdapter {
    /** */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    @IgniteJobContextResource
    private ComputeJobContext ctx;

    /** {@inheritDoc} */
    @Override public Serializable execute() throws IgniteCheckedException {
        UUID nodeId = ignite.configuration().getNodeId();

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
