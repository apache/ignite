/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.proto;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

/**
 * Job status task.
 */
public class GridHadoopProtocolJobStatusTask extends GridHadoopProtocolTaskAdapter<GridHadoopJobStatus> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default poll delay */
    private static final long DFLT_POLL_DELAY = 100L;

    /** Attribute for held status. */
    private static final String ATTR_HELD = "held";

    /** {@inheritDoc} */
    @Override public GridHadoopJobStatus run(final GridComputeJobContext jobCtx, GridHadoop hadoop,
        GridHadoopProtocolTaskArguments args) throws GridException {
        UUID nodeId = UUID.fromString(args.<String>get(0));
        Integer id = args.get(1);
        Long pollDelay = args.get(2);

        assert nodeId != null;
        assert id != null;

        GridHadoopJobId jobId = new GridHadoopJobId(nodeId, id);

        if (pollDelay == null)
            pollDelay = DFLT_POLL_DELAY;

        if (pollDelay > 0) {
            GridFuture<?> fut = hadoop.finishFuture(jobId);

            if (fut != null) {
                if (fut.isDone() || F.eq(jobCtx.getAttribute(ATTR_HELD), true))
                    return hadoop.status(jobId);
                else {
                    fut.listenAsync(new IgniteInClosure<GridFuture<?>>() {
                        @Override public void apply(GridFuture<?> fut0) {
                            jobCtx.callcc();
                        }
                    });

                    jobCtx.setAttribute(ATTR_HELD, true);

                    return jobCtx.holdcc(pollDelay);
                }
            }
            else
                return null;
        }
        else
            return hadoop.status(jobId);
    }
}
