/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.proto;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * Job status task.
 */
public class GridHadoopProtocolJobStatusTask extends GridHadoopProtocolTaskAdapter<GridHadoopJobStatus> {
    /** {@inheritDoc} */
    @Override public GridHadoopJobStatus run(final GridComputeJobContext jobCtx, GridHadoop hadoop,
        GridHadoopProtocolTaskArguments args) throws GridException {
        UUID nodeId = UUID.fromString(args.<String>get(0));
        int id = args.get(1);
        Long pollDelay = args.get(2);

        GridHadoopJobId jobId = new GridHadoopJobId(nodeId, id);

        if (pollDelay == null)
            pollDelay = hadoop.configuration().getJobStatusPollDelay();

        if (pollDelay > 0) {
            GridFuture<?> fut = hadoop.finishFuture(jobId);

            if (fut != null) {
                if (fut.isDone() || jobCtx.heldcc() )
                    return hadoop.status(jobId);
                else {
                    fut.listenAsync(new GridInClosure<GridFuture<?>>() {
                        @Override public void apply(GridFuture<?> fut0) {
                            jobCtx.callcc();
                        }
                    });

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
